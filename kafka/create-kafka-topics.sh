#!/bin/bash

BROKER="kafka:29092"

# Create topics
TOPICS=(
    "logs.fe.network"
    "logs.fe.application"
    "logs.be.network"
    "logs.be.application"
)

for topic in "${TOPICS[@]}"; do
  kafka-topics.sh --create --bootstrap-server $BROKER --replication-factor 1 --partitions 3 --topic $topic
done
