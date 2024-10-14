FROM openjdk:17-slim-bullseye

RUN apt-get update && \
    apt-get install -y curl

ENV KAFKA_VERSION 3.8.0
ENV SCALA_VERSION 2.13

RUN  mkdir /tmp/kafka && \
    curl "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    -o /tmp/kafka/kafka.tgz && \
    mkdir /kafka && cd /kafka && \
    tar -xvzf /tmp/kafka/kafka.tgz --strip 1

RUN mkdir -p /data/kafka

COPY start-kafka.sh  /usr/bin

RUN chmod +x  /usr/bin/start-kafka.sh

WORKDIR /kafka/bin

CMD ["start-kafka.sh"]
