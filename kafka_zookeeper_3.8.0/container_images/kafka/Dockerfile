FROM openjdk:11-jre-slim

ENV KAFKA_VERSION=3.8.0
ENV SCALA_VERSION=2.13
ENV KAFKA_HOME=/opt/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin

RUN apt-get update && apt-get install -y \
    wget \
    tar \
    netcat \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://downloads.apache.org/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz \
    && tar -xvzf kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz \
    && mv kafka_$SCALA_VERSION-$KAFKA_VERSION /opt/kafka \
    && rm kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz

EXPOSE 9092

COPY server.properties /opt/kafka/config/server.properties

WORKDIR /opt/kafka/bin

CMD ["/opt/kafka/bin/kafka-server-start.sh", "/opt/kafka/config/server.properties"]