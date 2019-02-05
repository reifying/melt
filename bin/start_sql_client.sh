#!/bin/bash

docker run -it --rm microsoft/mssql-tools /opt/mssql-tools/bin/sqlcmd -S `ipconfig getifaddr en0` -U sa -P password-1234
