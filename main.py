from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging
import uuid
import random
import time
import json
import threading

KAFKA_BROKERS = "host.docker.internal:29092,host.docker.internal:39092,host.docker.internal:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = 'financial_transaction'

logging.basicConfig(
    level=logging.INFO,

)

logger = logging.getLogger(__name__)

producer_conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'queue.buffering.max.messages':10000,
    'queue.buffering.max.kbytes':512000,
    'batch.num.messages':1000,
    'linger.ms':50,  #expensive in production setting
    'acks':1,
    'compression.type':'gzip',
    'request.timeout.ms': 60000,  # Increase timeout
    'retries': 5,  # Retry failed requests
}

producer = Producer(producer_conf)

def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})

    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR,
            )
            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()  # Block until the topic is created
                    logger.info(f"Topic '{topic_name}' created successfully!")
                except Exception as e:
                    logger.info(f"Failed to create topic '{topic_name}': {e}")
        else:
            logger.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.info(f"Failed to create topic '{topic_name}': {e}")

def generate_transaction():
    return dict(
        transactiionId = str (uuid.uuid4()),
        userId = f"user_{random.randint(1,100)}",
        amount = round(random.uniform(50000,150000),2),
        transactionTime = int(time.time()),
        merchartId = random.choice(['merchant_1','merchant_2','merchant_3']),
        transactionType = random.choice(['purchase','refund']),
        location = f'location_{random.randint(1,50)}',
        paymentMethod = random.choice(['credit_card','paypal','bank_transfer']),
        isInternational = random.choice(['True','False']),
        currentcy = random.choice(['USD','EUR','GBP'])
            )

def delivery_report(err,msg):
    key_ = msg.key()
    if err is not None:
        print(f'Delivery failed for record {key_}')
    else:
        print(f'Record {key_} successfully produced')

def produce_transaction(thread_id):
    while True:
        transaction = generate_transaction()

        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=transaction['userId'],
                value=json.dumps(transaction).encode('utf-8'),
                on_delivery=delivery_report
            )
            print(f'thread {thread_id} -Produced transaction: {transaction}')
            producer.flush()

        except Exception as e:
            print(f'Error sending transaction: {e}')


def producer_data_in_parallel(nums_threads):
    threads = []

    try:
        for i in range(nums_threads):
            thread = threading.Thread(target =produce_transaction,args=(i,))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    except Exception as e:
        print('Error message {e]}')

# Example usage
if __name__ == "__main__":
    create_topic(TOPIC_NAME)

    producer_data_in_parallel(3)
