from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = "financial_transactions"
AGGREGATES_TOPIC = "transaction_aggregates"
ANOMALIES_TOPIC = "transaction_anomalies"
CHECKPOINT_DIR = "/mnt/spark-checkpoints"
STATES_DIR = "/mnt/spark-state"

spark = (
    SparkSession.builder.appName("FinancialTransactionsProcessor")
    .config(
        "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )  # https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.13/3.5.0    docker compose using spark 3.5.0
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
    .config("spark.sql.streaming.stateStore.stateStoreDir", STATES_DIR)
    .config("spark.sql.shuffle.partitions", 20)
).getOrCreate()

transaction_schema = StructType(
    [
        StructField(name="transactionId", dataType=StringType(), nullable=True),
        StructField(name="userId", dataType=StringType(), nullable=True),
        StructField(name="merchantId", dataType=StringType(), nullable=True),
        StructField(name="amount", dataType=DoubleType(), nullable=True),
        StructField(name="transactionTime", dataType=LongType(), nullable=True),
        StructField(name="transactionType", dataType=StringType(), nullable=True),
        StructField(name="location", dataType=StringType(), nullable=True),
        StructField(name="paymentMethod", dataType=StringType(), nullable=True),
        StructField(name="isInternational", dataType=StringType(), nullable=True),
        StructField(name="currency", dataType=StringType(), nullable=True),
    ]
)



try:
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
except Exception as e:
    print(f"Error connecting to Kafka: {str(e)}")
    exit(1)


transactions_df = (
    kafka_stream.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), transaction_schema).alias("data"))
    .select("data.*")
)

transactions_df = transactions_df.withColumn(
    "transactionTimestamp",
    from_unixtime(col("transactionTime") / 1000).cast("timestamp"),
)

aggregated_df = (
    transactions_df
        .groupBy(coalesce(col("merchantId"), lit("UNKNOWN")).alias("merchantId"))
        .agg(
            sum(coalesce(col("amount"), lit(0))).alias("totalAmount"),
            count("*").alias("transactionCount"),
        )
)

aggregation_query = (
    aggregated_df
    .withColumn("key", col("merchantId").cast("string"))
    .withColumn(
        "value",
        to_json(
            struct(
                col("merchantId"),
                col("totalAmount"),
                col("transactionCount"),
            )
        ),
    )
    .selectExpr("key", "value")
    .writeStream
    .format("kafka")
    .outputMode("update")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("topic", AGGREGATES_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregates")
    .start()
    .awaitTermination()
)