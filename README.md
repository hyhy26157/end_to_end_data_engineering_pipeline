

python v -3.13.1


## KAFKA CONTROLLER SETUP

 Imagine Kafka is a Pizza Delivery Company:
In this company:

Kafka Nodes = Pizza Shops
Controller Node = The Manager in charge of coordinating all the pizza shops.
Environment Variables = The instructions given to the pizza shop (or node) to run properly.
Let’s break down the instructions (environment variables) you're setting for the Kafka Controller Node:

📋 Kafka Environment Variables (with a Pizza Twist):
Environment Variable	Explanation (ELI5)
KAFKA_NODE_ID=1	🆔 Shop Number 1 — This node is like the first pizza shop in your network. Every shop needs an ID!
KAFKA_PROCESS_ROLES=controller	👨‍💼 Manager Role — This node is the manager of all the pizza shops. It makes sure everyone is working together.
KAFKA_LISTENERS=CONTROLLER://:9093	📞 Phone Number for the Manager — This is the phone line (port 9093) where the manager listens for updates.
KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT	📬 Message Type Between Shops — The pizza shops send plain text messages to each other (no encryption needed).
KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER	🎧 Who is Listening? — The manager node listens on the controller line (like a special phone line for the manager).
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0	⏱️ No Waiting — If one pizza shop stops working, the manager doesn’t wait to fix the issue. The rebalancing happens immediately.
KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-controller-1:9093	✅ Voting for Decisions — The manager node needs to make decisions. In this case, there’s only 1 voter (the manager itself).
💡 What Does This Do?
This setup is telling Kafka to:

Create a controller node (the manager) to oversee the cluster.
Assign a unique ID to this controller node (Node ID = 1).
Set up the communication channels between the manager and other nodes.
Make decisions immediately without waiting when something goes wrong.
Use only one voter (the manager itself) for important decisions.
🚀 In Short:
You're creating a Kafka manager node that will act like the boss of a pizza delivery company, ensuring that all pizza shops (nodes) work together smoothly without delays.

Let me know if you'd like more technical details or a visual representation! 😊

#### configuration

- docker compose up -d
- docker compose down

##### clean docker

- docker ps
- docker exec -it kafka-broker-1 bash


#### java configuration - to create producer

- mvn clean install   (full)
- mvn compile  (fast)

- mvn exec:java


### pyspark processor configuration

cd to/jobs/ where your spark_processor.py is

step 1
- docker build -t spark-processor-image .

- rebuild docker when things changed - docker build -t spark-processor-image .


step 2
docker exec -it end_to_end_data_engineering_pipeline-spark-master-1 spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    //opt/bitnami/spark/jobs/spark_processor.py



