#!/usr/bin/env bash

set -e

docker-compose down
docker-compose up -d

TOPIC=test

until kafka-topics.sh --create --replication-factor 1 --partitions 1 --topic $TOPIC --zookeeper localhost:2181
do
  sleep 1
  echo "Couldn't create topic, retrying..."
done

