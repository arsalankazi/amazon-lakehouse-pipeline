Write-Host ">>> Starting Bronze Ingestion Stream..." -ForegroundColor Cyan
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
  /opt/spark/work-dir/src/streaming_bronze.py
