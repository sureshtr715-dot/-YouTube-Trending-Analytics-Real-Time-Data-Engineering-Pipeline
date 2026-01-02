import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType
)

# ===============================
# Config (ENV-based)
# ===============================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "youtube_trending")
BASE_PATH = os.getenv("DATA_LAKE_PATH", "delta-lake")

BRONZE_PATH = f"{BASE_PATH}/bronze/youtube_trending"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints/youtube_trending"

# ===============================
# Spark Session (Delta enabled)
# ===============================
spark = (
    SparkSession.builder
    .appName("YouTubeTrendingToDelta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===============================
# Kafka Stream
# ===============================
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# ===============================
# Schema (Clean & Correct)
# ===============================
schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("viewCount", LongType(), True),
    StructField("likeCount", LongType(), True),
    StructField("ingested_at", StringType(), True)
])

# ===============================
# Parse + Flatten
# ===============================
parsed_df = (
    kafka_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("processing_time", current_timestamp())
)

# ===============================
# Write to Delta (Bronze)
# ===============================
query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .start(BRONZE_PATH)
)

query.awaitTermination()


