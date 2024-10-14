FROM openjdk:11-jre-slim

ENV ZOOKEEPER_VERSION=3.7.2
ENV ZOOKEEPER_HOME=/opt/zookeeper
ENV PATH=$PATH:$ZOOKEEPER_HOME/bin

RUN apt-get update && apt-get install -y \
    wget \
    tar \
    netcat \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://downloads.apache.org/zookeeper/zookeeper-$ZOOKEEPER_VERSION/apache-zookeeper-$ZOOKEEPER_VERSION-bin.tar.gz \
    && tar -xvzf apache-zookeeper-$ZOOKEEPER_VERSION-bin.tar.gz \
    && mv apache-zookeeper-$ZOOKEEPER_VERSION-bin /opt/zookeeper \
    && rm apache-zookeeper-$ZOOKEEPER_VERSION-bin.tar.gz

COPY zoo.cfg $ZOOKEEPER_HOME/conf/zoo.cfg

EXPOSE 2181

CMD ["zkServer.sh", "start-foreground"]