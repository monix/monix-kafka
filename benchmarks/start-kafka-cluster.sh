#!/usr/bin/env bash

set -e

function create_topic {
    TOPIC_NAME=$1
    PARTITIONS=$2
    REPLICATION_FACTOR=$3
    echo "Creating topic ${TOPIC_NAME} with ${PARTITIONS} partitions and replication factor of ${REPLICATION_FACTOR}."
    docker-compose -f ./docker-compose.yml exec -T broker kafka-topics --create --topic ${TOPIC_NAME} --partitions ${PARTITIONS} --replication-factor ${REPLICATION_FACTOR} --if-not-exists --zookeeper zookeeper:2181
}

echo "Starting Kafka cluster..."
docker-compose -f ./docker-compose.yml up -d zookeeper broker

echo -e "Docker ps..."
docker ps

sleep 15

create_topic topic_producer_1P_1RF 1 1
create_topic topic_producer_2P_1RF 2 1
create_topic topic_sink_1P_1RF 1 1
create_topic topic_sink_2P_1RF 2 1
create_topic topic_consumer_1P_1RF 1 1
create_topic topic_consumer_2P_1RF 2 1