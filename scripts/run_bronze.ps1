$awsKey = if ($env:AWS_ACCESS_KEY_ID) { $env:AWS_ACCESS_KEY_ID } else { "test" }
$awsSecret = if ($env:AWS_SECRET_ACCESS_KEY) { $env:AWS_SECRET_ACCESS_KEY } else { "test" }
$s3Endpoint = if ($env:S3_ENDPOINT_URL) { $env:S3_ENDPOINT_URL } else { "http://localstack-s3:4566" }
$bucket = if ($env:LAKEHOUSE_BUCKET) { $env:LAKEHOUSE_BUCKET } else { "amazon-lakehouse" }

Write-Host ">>> Initializing Bronze Ingestion Stream (Kafka -> Parquet)..." -ForegroundColor Cyan

docker exec -it `
  -e AWS_ACCESS_KEY_ID=$awsKey `
  -e AWS_SECRET_ACCESS_KEY=$awsSecret `
  -e S3_ENDPOINT_URL=$s3Endpoint `
  -e LAKEHOUSE_BUCKET=$bucket `
  spark-master /opt/spark/bin/spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
  /opt/spark/work-dir/src/streaming_bronze.py
