#!/bin/bash

docker stop kafka-broker zookeeper
docker rm kafka-broker zookeeper
rm -rf kafka-docker
