#!/bin/bash
set -ex
sudo aws s3 cp s3://{{bucket}}/emr-bootstrap/iceberg.properties /etc/trino/conf/catalog/iceberg.properties