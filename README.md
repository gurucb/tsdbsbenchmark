# Benchmarking tool for Timeseries databases

Benchmarking too for Timeseries databases is used to load timeseries database with different load and access patterns. For initial iteration of tests we are considering 
1. InfluxDB
2. PG + Timescale Extensions
3. Crate.io

Benchmark tool capabilities will be across 5 different components.

1. **Data Generator**: To generate sample payload for ingestion
2. **Data Loader**: Bulk and singleton ingestion to Timeseries databases
3. **Query Generator**: Generate queries based on template and data apriori.
4. **Query Executor**: Execute queries in parallel and measure clock times and asynchronously write to files.
5. **Result Aggregator**: For each run process results and generate summaries and reports.

All of above components will have to leverage common horizontal components for configuration, logging, measuring, capturing and storing execution times. Below we will define components responsibilities in more detail.  

1. *Component: Data Generation*        
    1. Driven by metadata JSON
    2. Generates data that creates scenarios and use cases to be tested.
    3. Should be replicated by X number. Example, 
        1. Create data load for 1 energy meter for 1 year. (1 Meter Data for year = 365 x 24 x 60 x (6 to 60) seconds)
        2. We replicate same code for say 1000 energy meters.
    4. Data Generated should be ready for bulk loading format for each database and this again should be controlled by JSON (easy to add new database)
        1. Influx: Line Protocol https://docs.influxdata.com/influxdb/v0.12/guides/writing_data/#writing-multiple-points 
        2. Postgres SQL: JSON
        3. Crate: JSON
    5. File shards should be created parallel and not single file.
        1. Shard Size should be configurable either in size or no. of points.

2.  *Component: Data Loader* (Pure Ingestion)
    1. Bulk files created by first component should be bulk loaded into  individual databases in parallel.
    2. Both local and remove bulk loading should be supported.
    3. Each DB should have its own bulk load module and best practices configured.
        Links for best practices configurations.
    4. Also there must be module to delete databases / buckets are bulk load is over. (We start afresh every time)
    5. Bulk load will be stated with various parameters optimal for each database (configurations to be taken from JSON)
    6. Parallelism should be configured.

3. *Component: Query Generator* 
    1. TODO

4. *Component: Query Execution*
    1. TODO

5. *Component: Result Aggregator and Reportor*
    1. TODO
