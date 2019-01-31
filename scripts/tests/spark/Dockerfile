FROM alpine:latest

WORKDIR /opt

ENV PATH="/opt/spark/bin:${PATH}"

RUN apk add --update --no-cache openssl openssl-dev nghttp2-dev ca-certificates bash openjdk8 libc6-compat && \
    wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz && \
    tar -xzf spark-2.3.1-bin-hadoop2.7.tgz && \
    ln -sf spark-2.3.1-bin-hadoop2.7 spark && \
    rm spark-2.3.1-bin-hadoop2.7.tgz
