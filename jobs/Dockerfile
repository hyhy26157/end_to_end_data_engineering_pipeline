# Use the Bitnami Spark image as the base
FROM bitnami/spark:latest

# Set the working directory inside the container
WORKDIR /app

# Copy your Spark job (spark_processor.py) into the container
COPY spark_processor.py /app/spark_processor.py

# Specify the default command to run your Spark job
ENTRYPOINT ["spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4", "/app/spark_processor.py"]
