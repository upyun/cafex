FROM frolvlad/alpine-oraclejdk8:cleaned

ENV KAFKA_VERSION="0.9.0.0" SCALA_VERSION="2.11"

RUN apk add --update wget docker bash jq && \
  rm -rf /var/cache/apk/* && \
  mkdir /opt && \
  wget -q -O - http://mirror.bit.edu.cn/apache/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz | tar xzf - -C /opt && \
  apk del wget

VOLUME ["/kafka"]

ENV KAFKA_HOME /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}
ADD start-kafka.sh /usr/bin/start-kafka.sh
ADD broker-list.sh /usr/bin/broker-list.sh
CMD start-kafka.sh
