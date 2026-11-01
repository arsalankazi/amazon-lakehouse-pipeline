$awsKey = if ($env:AWS_ACCESS_KEY_ID) { $env:AWS_ACCESS_KEY_ID } else { "test" }
$awsSecret = if ($env:AWS_SECRET_ACCESS_KEY) { $env:AWS_SECRET_ACCESS_KEY } else { "test" }
$s3Endpoint = if ($env:S3_ENDPOINT_URL) { $env:S3_ENDPOINT_URL } else { "http://localstack-s3:4566" }
$bucket = if ($env:LAKEHOUSE_BUCKET) { $env:LAKEHOUSE_BUCKET } else { "amazon-lakehouse" }

Write-Host ">>> Executing Gold Analytics Aggregation Batch (KPIs & Metrics)..." -ForegroundColor Cyan

docker exec -it `
  -e AWS_ACCESS_KEY_ID=$awsKey `
  -e AWS_SECRET_ACCESS_KEY=$awsSecret `
  -e S3_ENDPOINT_URL=$s3Endpoint `
  -e LAKEHOUSE_BUCKET=$bucket `
  spark-master /opt/spark/bin/spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.1.0 `
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
  /opt/spark/work-dir/src/batch_gold.py
