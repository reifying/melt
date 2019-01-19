#!/bin/bash

set +e
# Check zookeeper until broker is seen (e.g., /brokers/ids/1001)
for ((i=0; i<10; i++))
  do 
    echo dump | nc localhost 2181 | grep brokers && break
    sleep 1
done

set -e
for TOPIC in \
    "melt.SalesLT.CustomerAddress" \
    "melt.SalesLT.ProductModelProductDescription" \
    "melt.SalesLT.Product" \
    "melt.dbo.BuildVersion" \
    "melt.SalesLT.SalesOrderHeader" \
    "melt.SalesLT.ProductDescription" \
    "melt.SalesLT.Customer" \
    "melt.SalesLT.ProductModel" \
    "melt.SalesLT.SalesOrderDetail" \
    "melt.SalesLT.Address" \
    "melt.SalesLT.ProductCategory" \
    "melt.dbo.ErrorLog"
  do
    docker exec -it kafka-broker /bin/sh -c "/opt/kafka/bin/kafka-topics.sh --zookeeper \$KAFKA_ZOOKEEPER_CONNECT --create --partitions 1 --replication-factor 1 --topic $TOPIC"
done

