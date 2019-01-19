#!/bin/bash

set -e
docker pull wurstmeister/kafka
git clone https://github.com/wurstmeister/kafka-docker.git
cd kafka-docker
sed -i "s/KAFKA_ADVERTISED_HOST_NAME:.*/KAFKA_ADVERTISED_HOST_NAME: `hostname`/" docker-compose.yml
sed -i "s/kafka:/kafka:\n    container_name: kafka-broker/" docker-compose.yml
cat docker-compose.yml
docker-compose up -d
