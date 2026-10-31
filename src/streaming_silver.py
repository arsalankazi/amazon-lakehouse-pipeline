import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    DateType
)

# 1. Packages for S3A + Delta Lake 3.1.0
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
    'io.delta:delta-spark_2.12:3.1.0 '
    'pyspark-shell'
)

# 2. Spark Session with Delta Extensions
spark = SparkSession.builder \
    .appName("Amazon-Lakehouse-Silver-Delta") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack-s3:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(">>> Silver Delta Lake Ingestion Engine Initialized!")

# 3. Bronze Schema
bronze_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("device", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("event_date", DateType(), True)
])

# 4. Stream from Bronze Parquet
bronze_stream = spark.readStream \
    .schema(bronze_schema) \
    .parquet("s3a://amazon-lakehouse/bronze/events/")

# 5. Cleansing, Watermarking & Enrichment
clean_stream = bronze_stream \
    .filter(col("event_id").isNotNull() & (col("price") > 0)) \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["event_id", "timestamp"]) \
    .withColumn("event_hour", hour(col("timestamp"))) \
    .withColumn("is_high_value", when(col("price") >= 200.0, True).otherwise(False))

# 6. S3 Silver Delta Target & Checkpoint
s3_silver_delta_path = "s3a://amazon-lakehouse/silver/delta_events/"
checkpoint_silver_path = "s3a://amazon-lakehouse/checkpoints/silver_delta_v1/"

print(f">>> Streaming Deduplicated Silver Data into REAL DELTA TABLE: {s3_silver_delta_path}")

# 7. Write Stream as DELTA
query = clean_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .partitionBy("event_date") \
    .option("checkpointLocation", checkpoint_silver_path) \
    .trigger(processingTime="15 seconds") \
    .start(s3_silver_delta_path)

query.awaitTermination()