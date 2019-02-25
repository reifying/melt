#!/bin/bash

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
    docker exec -it kafka-broker /bin/sh -c "/opt/kafka/bin/kafka-topics.sh --zookeeper \$KAFKA_ZOOKEEPER_CONNECT --delete --topic $TOPIC"
done

