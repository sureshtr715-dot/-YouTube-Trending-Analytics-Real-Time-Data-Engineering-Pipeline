import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max, avg, count, date_trunc
)

# ===============================
# Config (ENV based)
# ===============================
BASE_PATH = os.getenv("DATA_LAKE_PATH", "delta-lake")

SILVER_PATH = f"{BASE_PATH}/silver/youtube_trending"
GOLD_CHANNEL_PATH = f"{BASE_PATH}/gold/channel_performance"
GOLD_LEADERBOARD_PATH = f"{BASE_PATH}/gold/trending_leaderboard"
GOLD_HOURLY_PATH = f"{BASE_PATH}/gold/hourly_growth"

# ===============================
# Spark Session
# ===============================
spark = (
    SparkSession.builder
    .appName("YouTubeSilverToGold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===============================
# Load Silver
# ===============================
silver_df = spark.read.format("delta").load(SILVER_PATH)

# ===============================
# GOLD 1 — Channel Performance
# ===============================
channel_gold_df = (
    silver_df
    .groupBy("channel")
    .agg(
        max("viewCount").alias("max_views"),
        avg("likeCount").alias("avg_likes"),
        count("video_id").alias("video_count")
    )
)

channel_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_CHANNEL_PATH)

print("✅ Gold table written: channel_performance")

# ===============================
# GOLD 2 — Trending Leaderboard
# ===============================
leaderboard_gold_df = silver_df.select(
    "video_id",
    "title",
    "channel",
    "viewCount",
    "likeCount",
    "published_at"
)

leaderboard_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_LEADERBOARD_PATH)

print("✅ Gold table written: trending_leaderboard")

# ===============================
# GOLD 3 — Hourly Growth
# ===============================
hourly_growth_df = (
    silver_df
    .withColumn("hour", date_trunc("hour", col("published_at")))
    .groupBy("hour", "video_id")
    .agg(max("viewCount").alias("max_views_hour"))
)

hourly_growth_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_HOURLY_PATH)

print("✅ Gold table written: hourly_growth")

print("\n🎯 ALL GOLD TABLES GENERATED SUCCESSFULLY")
