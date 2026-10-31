import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, round as _round, countDistinct, when

# 1. Delta & S3 Packages
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
    'io.delta:delta-spark_2.12:3.1.0 '
    'pyspark-shell'
)

# 2. Spark Session
spark = SparkSession.builder \
    .appName("Amazon-Lakehouse-Gold-Aggregation") \
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
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(">>> Gold Analytics Engine Initialized! Reading from Silver Delta...")

# 3. Read Directly From Delta Table
silver_delta_path = "s3a://amazon-lakehouse/silver/delta_events/"
silver_df = spark.read.format("delta").load(silver_delta_path)

print(f">>> Total records loaded from Silver Delta Lake: {silver_df.count()}")

# 4. Aggregation 1: Category-wise Revenue & Conversion Metrics
gold_category_metrics = silver_df.groupBy("category") \
    .agg(
        count("event_id").alias("total_events"),
        countDistinct("user_id").alias("unique_users"),
        _round(_sum(when(col("event_type") == "purchase", col("price")).otherwise(0.0)), 2).alias("total_revenue"),
        count(when(col("event_type") == "purchase", True)).alias("total_purchases")
    ) \
    .withColumn(
        "conversion_rate_pct",
        _round((col("total_purchases") / col("total_events")) * 100, 2)
    )

print(">>> Category Metrics Preview:")
gold_category_metrics.show(truncate=False)

# 5. Write to Gold Layer
gold_output_path = "s3a://amazon-lakehouse/gold/category_metrics/"
gold_category_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_output_path)

print(f">>> Successfully written aggregated KPI metrics to Gold Delta: {gold_output_path}")

spark.stop()