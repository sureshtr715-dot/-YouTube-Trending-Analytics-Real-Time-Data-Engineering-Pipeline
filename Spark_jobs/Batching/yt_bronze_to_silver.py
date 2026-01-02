import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp
)
from delta.tables import DeltaTable

# ===============================
# Config (ENV-based)
# ===============================
BASE_PATH = os.getenv("DATA_LAKE_PATH", "delta-lake")

BRONZE_PATH = f"{BASE_PATH}/bronze/youtube_trending"
SILVER_PATH = f"{BASE_PATH}/silver/youtube_trending"

# ===============================
# Spark Session
# ===============================
spark = (
    SparkSession.builder
    .appName("YouTubeBronzeToSilver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===============================
# Read Bronze
# ===============================
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

# ===============================
# Clean + Transform
# ===============================
silver_df = (
    bronze_df
    .withColumn("viewCount", col("viewCount").cast("long"))
    .withColumn("likeCount", col("likeCount").cast("long"))
    .withColumn(
        "published_at",
        to_timestamp("published_at", "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    .withColumn("ingestion_time", current_timestamp())
    .dropDuplicates(["video_id"])
)

# ===============================
# Merge into Silver (Upsert)
# ===============================
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    (
        silver_table.alias("s")
        .merge(
            silver_df.alias("b"),
            "s.video_id = b.video_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("✅ Silver layer updated successfully")
