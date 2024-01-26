#!/bin/bash

set -ex

JARS=$(find /opt/spark/deps -type f -name "*.jar" | tr '\n' ':')

/opt/spark/sbin/start-connect-server.sh  \
  --master local[3] \
  --driver-class-path $JARS \
  --conf spark.driver.extraJavaOptions="-Djdbc.drivers=org.postgresql.Driver" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.catalog.risingwave=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.risingwave.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
  --conf spark.sql.catalog.risingwave.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.risingwave.warehouse=s3://icebergdata/demo \
  --conf spark.sql.catalog.risingwave.uri=jdbc:postgresql://postgres:5432/iceberg \
  --conf spark.sql.catalog.risingwave.jdbc.user=admin \
  --conf spark.sql.catalog.risingwave.jdbc.password=123456 \
  --conf spark.sql.catalog.risingwave.s3.endpoint=http://minio-0:9301 \
  --conf spark.sql.catalog.risingwave.s3.path.style.access=true \
  --conf spark.sql.catalog.risingwave.s3.access.key=hummockadmin \
  --conf spark.sql.catalog.risingwave.s3.secret.key=hummockadmin \
  --conf spark.sql.defaultCatalog=risingwave

tail -f /opt/spark/logs/spark*.out