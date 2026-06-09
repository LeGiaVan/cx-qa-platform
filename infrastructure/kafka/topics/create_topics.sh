#!/bin/bash
# Chạy sau khi Kafka đã up

KAFKA_BIN=/usr/bin/kafka-topics
BOOTSTRAP=localhost:9092

topics=(
  "conversations.raw"
  "conversations.flagged"
  "customer.profile.changes"
)

for topic in "${topics[@]}"; do
  $KAFKA_BIN --create \
    --bootstrap-server $BOOTSTRAP \
    --topic $topic \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists
  echo "Created: $topic"
done