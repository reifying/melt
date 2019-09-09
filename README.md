# melt
Migrate DB tables to Kafka topics (or melt the tables into streams). Support the common tasks to perform migrations:
* planning and analysis
* initial load
* re-sync to catch up when differences exist
* verification
* monitoring


## Motivation

Existing Kafka connectors for databases (JDBC or more specialized) require some
specific traits about table design in order to reliably copy table updates to
a topic. See section on "Timestamp and Incrementing Columns" in [JDBC Connector
Documentation](https://docs.confluent.io/5.1.0/connect/kafka-connect-jdbc/source-connector/index.html)
on why having less than this configuration could result in missed updates.
Although this is a strong approach, a legacy database will likely fall short of
this requirement, and it can both be a costly investment and increase risk to
modify the existing schema in preparation for Kafka propagation. Yet, Kafka can
be a great tool to use in a strategy to migrate away from a legacy database.

Melt is trying to fill in the gap in existing Kafka connector capabilities. It
may be used in tandem with other connectors, completing the synchronization for
tables missing primary keys, sufficiently unique timestamps, etc.


## Initial assumptions

The current implementation assumes that a table and the latest state (based on
primary keys) of a topic can be contained in memory. This may latter be moved to
overflow to a disk or other cache.

The primary focus is change data replication where log compaction can be run
frequently (i.e. daily) to avoid long topic load times.


## Usage

See ``jdbc.melt.integration-test`` for a demonstration of features. Steps to
configure a local environment to run integration tests will be provided soon.
Meanwhile, refer to ``.travis.yml`` and the referenced scripts to initialize
an environment via Docker.
