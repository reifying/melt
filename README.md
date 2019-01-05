# melt
Migrate DB tables to Kafka topics (or melt the tables into streams). Support the common tasks to perform migrations:
* planning and analysis
* initial load
* re-sync to catch up when differences exist
* verification
* monitoring

## How to use it
1. Set required environment variables:
   * TEST_MSSQL_HOST
   * TEST_MSSQL_USER
   * TEST_MSSQL_PASS
   * TEST_MSSQL_NAME

2. Save current schema to a file
   (optional step for future comparison when schema changes)
    ```
    clj -i src/melt/analyze.clj  -e "(melt.analyze/save-schema)"
    ```

3. Take a row-count and sample of the data from all user tables
    ```
    clj -m melt.analyze
    ```


*Complete:*

## Plan Transformations
1. Read schema from db
2. Compare schema to local cache
3. Show diff if different
4. Proceed if different based on env var
5. Read top 10 records from all tables
6. pprint samples in files of corresponding names
7. (Manually) decide how to transform them

*Remaining:*

## Load topics
1. Steps 1-4 from plan
2. Fully read topics
3. Read tables
4. Transform table data
5. Diff table to topic
6. Send deltas to topics

## Verify
Steps 1-5 from Load

## Monitor
Verify, allowing for some latency

## Real-time sync
0. Enable change tracking for tables with primary keys (SQL Server)
1. On new message, find schema for table changed
2. Read table for record that changed
3. Transform
4. Send to topic


