package com.datamasterylab;

import com.datamasterylab.dto.Transaction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TransactionProducer class for producing financial transaction messages to a Kafka topic.
 */
public class TransactionProducer {

    // Logger instance for logging messages
    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);

    // Kafka topic and server configuration
    private static final String TOPIC = "financial_transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092,localhost:49092";

    // Producer and topic configuration
    private static final int NUM_THREADS = 3;
    private static final int NUM_PARTITIONS = 5;
    private static final short REPLICATION_FACTOR = 3;

    public static void main(String[] args) {

        // Step 1: Check and create topic if it doesn't exist
        createTopicIfNotExists();

        // Step 2: Kafka producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64 KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 3); // 3 ms
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // Step 3: Create a thread pool to send messages in parallel
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        ObjectMapper objectMapper = new ObjectMapper();

        for (int i = 0; i < NUM_THREADS; i++) {
            executor.submit(() -> {
                long startTime = System.currentTimeMillis();
                long recordsSent = 0;

                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                    while (true) {
                        // Generate a random transaction
                        Transaction transaction = Transaction.randomTransaction();
                        String transactionJson = objectMapper.writeValueAsString(transaction);

                        // Create a ProducerRecord and send it to Kafka
                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, transaction.getTransactionId(), transactionJson);
                        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                            if (exception != null) {
                                logger.error("Failed to send record with key {} due to {}", record.key(), exception.getMessage());
                            } else {
                                logger.info("Record with key {} sent to partition {} with offset {}", record.key(), metadata.partition(), metadata.offset());
                            }
                        });

                        // Increment record count
                        recordsSent++;

                        // Track and log throughput every second
                        long elapsedTime = System.currentTimeMillis() - startTime;
                        if (elapsedTime >= 1000) {
                            double throughput = recordsSent / (elapsedTime / 1000.0);
                            logger.info("Throughput: {} records/sec", throughput);
                            startTime = System.currentTimeMillis();
                            recordsSent = 0;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Producer encountered an error", e);
                }
            });
        }

        // Step 4: Add a shutdown hook to cleanly stop the producer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdownNow();
            logger.info("Producer stopped.");
        }));
    }

    /**
     * Method to check if the Kafka topic exists, and create it if it doesn't.
     */
    private static void createTopicIfNotExists() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            boolean topicExists = adminClient.listTopics().names().get().contains(TOPIC);

            if (!topicExists) {
                logger.info("Topic '{}' does not exist. Creating topic...", TOPIC);
                NewTopic newTopic = new NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                logger.info("Topic '{}' created successfully.", TOPIC);
            } else {
                logger.info("Topic '{}' already exists.", TOPIC);
            }
        } catch (Exception e) {
            logger.error("Error checking or creating topic", e);
        }
    }
}
