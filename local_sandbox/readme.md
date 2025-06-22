## Setup

For setting up the Hive metastore, the user must add the hadoop-aws and aws-java-sdk-bundle jars to the `hive/` folder. This may be done by downloading them through the maven site or by running the following commands:

`wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar -O ./hive/hadoop-aws-3.3.6.jar`
`wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar -O ./hive/aws-java-sdk-bundle-1.12.367.jar`

The versions mentioned previously were validated to work with the docker images specified in the docker-compose file.

For the dataset, the script from `./generate_data` must be run and the resulting `.parquet` data files added to `./parsed_data/s_1/`.

## Usage

Run containers
`docker-compose -f docker-compose.yaml up`

Start trino client
`docker exec -it trino trino`

Add dataset files to minio
`docker exec -it mc bash`
`mc cp /parsed_data minio/warehouse/parsed_data --recursive`

Connect to hive directly
`docker exec -it hive-metastore hive`
`!connect 'jdbc:hive2://hive-metastore:9083/'`


