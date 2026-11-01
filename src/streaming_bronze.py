import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

# 1. Decoupled Environment Configurations
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localstack-s3:4566")
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "raw-ecommerce-events")
LAKEHOUSE_BUCKET = os.getenv("LAKEHOUSE_BUCKET", "amazon-lakehouse")

# 2. Packages configuration for Kafka & S3A
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
    'org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

# 3. Spark Session Initialization (Parameterized S3A Security)
spark = SparkSession.builder \
    .appName("Amazon-Lakehouse-Bronze-Ingestion") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f">>> Bronze Ingestion Engine Initialized! Connecting to Kafka: {KAFKA_BROKER}")

# 4. Ingestion Schema
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("device", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 5. Read from Kafka Stream
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 6. Extract JSON and add event_date partition column
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_date", to_date(col("timestamp")))

# 7. S3 Output Paths
s3_bronze_path = f"s3a://{LAKEHOUSE_BUCKET}/bronze/events/"
checkpoint_path = f"s3a://{LAKEHOUSE_BUCKET}/checkpoints/bronze_v1/"

print(f">>> Writing Bronze raw parquet events to: {s3_bronze_path}")

# 8. Start Streaming Query
query = parsed_stream.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .partitionBy("event_date") \
    .option("path", s3_bronze_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
