⚠️⚠️ Localhost connection is currently not working during the process. ⚠️⚠️

# Language and Systems Versions

| Component             | Version     |
|-----------------------|-------------|
| Kafka                 | 3.8.0       |
| Zookeeper             | 3.7.2       |
| Java                  | 11-jre-slim |
| Python                | 3.10.12     |
| Scala                 | 2.12.18     |
| Scala Kafka-Clients Jar| 3.8.0       |

# KAFKA COMMAND

## Create Topic
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cagri --replication-factor 3 --partitions 3

For zookeeper

./kafka-topics.sh --zookeeper zookeeper:2181 --create --topic cagri --partitions 1 --replication-factor 1

## List The Topics
./kafka-topics.sh --bootstrap-server localhost:9092 --list

## Start The Producer
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic cagri

## Start The Producer with Key and Seperator
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic cagri --property "parse.key=true" --property "key.separator=,"

## Start The Consumer
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cagri --from-beginning

## Show Describe The Topic
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cagri

## Delete The Topic
./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic cagri

## Upgrade The Topic (INCREASE)
./kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic cagri --partitions 5 

## Basic Example
/kafka/bin/kafka-topics.sh \ (1)
--bootstrap-server kafka:9092 \ (2)
--create --topic cagri \ (3)
--replication-factor 3 \ (4)
--partitions 3 (5)


1. This is the script to manage Kafka topics. It is located in the `/kafka/bin`
2. Kafka server's address (check the `/config/server.properties`)
3. This flag is used to create a new topic.
4. This defines the replication factor for the topic, which is the number of Kafka brokers that will replicate the data. (`Not: We deploy 3 broker so our max replication-factor is 3`)
5. This defines the number of partitions for the topic. Each partition allows parallelism in message consumption.

1. ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic dahbest --replication-factor 3 --partitions 3

2.
    1. terminal-1 = ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dahbest --from-beginning --partition 0 --property print.key=true
    2. terminal-2 = ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dahbest --from-beginning --partition 1 --property print.key=true
    3. terminal-3 = ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dahbest --from-beginning --partition 2 --property print.key=true

3. ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic dahbest --property "parse.key=true" --property "key.separator=,"

# Output:
## producer terminal

`docker exec -it kafka1 bash`

``./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic dahbest --property "parse.key=true" --property "key.separator=,"``

`1, hellow`

`2, I'm Cagri`

`3, I'm sure, I am, I`

`4, why didnt you send the message to partition 1`

## terminal-1:

`docker exec -it kafka1 bash`

``./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dahbest --from-beginning --partition 0 --property print.key=true``

`1        hellow`

## terminal-2:

`docker exec -it kafka2 bash`

``./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dahbest --from-beginning --partition 1 --property print.key=true``

`4        why didnt you send the message to partition 1`

## terminal-3:

`docker exec -it kafka3 bash`

``./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dahbest --from-beginning --partition 2 --property print.key=true``

`2        I'm Cagri`

`3        I'm sure, I am, I`
