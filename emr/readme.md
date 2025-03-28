This folder provides utilities to run EMR with trino installed with 2 glue catalog connectors, the default one for Hive tables and another for Iceberg tables.

Run this command to create a trino cluster. It must be in the same folder as the config.json file. Must add the bootstrap.sh file do the given s3 path as well.

`aws emr create-cluster --release-label emr-7.8.0 \
--applications Name=Trino \
--region us-east-2 \
--name My_Trino_Iceberg_Cluster \
--bootstrap-actions '[{"Path":"s3://{{bucket}}/emr-bootstrap/bootstrap.sh","Name":"Add iceberg.properties"}]' \
--configurations file://config.json \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=1,InstanceType=m5.xlarge --use-default-roles --ec2-attributes KeyName=hive
`

When in the cluster access trino by using the command `trino-cli`


`SET SESSION hive.non_transactional_optimize_enabled=true;`


To optimize Hive tables
`ALTER TABLE test_table EXECUTE optimize;`
