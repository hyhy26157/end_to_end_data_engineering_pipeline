from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging

KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = 'financial_transaction'

logging.basicConfig(
    level=logging.INFO,

)

logger = logging.getLogger(__name__)

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

# Example usage
if __name__ == "__main__":
    create_topic(TOPIC_NAME)
