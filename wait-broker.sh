#!/bin/sh
# wait-consumer.sh (updated)
until nc -z kafka 9092; do
  echo "Waiting for Kafka (port 9092)..."
  sleep 5
done
echo "Kafka is ready!"

if [ "$#" -gt 0 ]; then
  exec "$@"
fi

