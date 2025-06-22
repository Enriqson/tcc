Repository with source code for my Computer Engineering Graduate Thesis at CIn-Ufpe: Evaluating the performance between Apache Iceberg and Apache Hive utilizing star schema

`/benchmark` contains a python notebook for executing Star Schema Benchmark workloads on AWS Athena and another notebook for analyzing the output data.
`/generate_data` contains the scripts and instructions for gernerating Star Schema Benchmark data.
`/emr` contains unused configs for running emr with trino and glue data catalog
`/hive_catalog` contains unused dockerfile that created a hive catalog with jars and s3 configuration
`/local_sandbox` contains a local sandbox to run trino in a docker environment with minio and Iceberg and Hive metastores
