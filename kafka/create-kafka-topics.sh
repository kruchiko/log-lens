#!/bin/bash

BROKER="kafka:9092"

TOPICS=(
    "logs.fe.network"
    "logs.fe.application"
    "logs.be.network"
    "logs.be.application"
)

for topic in "${TOPICS[@]}"; do
  echo "Creating topic: $topic"
  kafka-topics.sh --create --bootstrap-server $BROKER --replication-factor 1 --partitions 3 --topic $topic || true
done
