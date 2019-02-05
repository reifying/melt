#!/bin/bash

docker pull robyvandamme/mssql-server-linux-adventureworks
set -e
docker run --rm -e 'ACCEPT_EULA=Y' -e "MSSQL_SA_PASSWORD=$TEST_MSSQL_PASS" -p 1433:1433 -d robyvandamme/mssql-server-linux-adventureworks
