Write-Host ">>> Starting Silver Delta Lake Streaming Engine..." -ForegroundColor Cyan
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.1.0 `
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
  /opt/spark/work-dir/src/streaming_silver.py