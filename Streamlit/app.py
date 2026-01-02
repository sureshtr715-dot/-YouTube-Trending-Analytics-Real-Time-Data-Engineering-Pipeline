import streamlit as st
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pathlib import Path

# ----------------------------------------
# Page Config
# ----------------------------------------
st.set_page_config(page_title="YouTube Trending Analytics", layout="wide")
st.title("📊 YouTube Trending Analytics (Gold Layer)")

# ----------------------------------------
# Resolve Project Root Dynamically
# ----------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DELTA_GOLD_PATH = PROJECT_ROOT / "delta-lake" / "gold"

# ----------------------------------------
# Spark Session (Cached)
# ----------------------------------------
@st.cache_resource
def get_spark():
    builder = (
        SparkSession.builder
        .appName("YouTube-Delta-Viewer")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

spark = get_spark()

# ----------------------------------------
# Gold Tables
# ----------------------------------------
TABLES = {
    "Trending Leaderboard": "trending_leaderboard",
    "Channel Performance": "channel_performance",
    "Hourly Growth": "hourly_growth"
}

selected_table = st.selectbox("Select Gold Table", list(TABLES.keys()))

table_path = DELTA_GOLD_PATH / TABLES[selected_table]

# ----------------------------------------
# Load & Display Data
# ----------------------------------------
df = spark.read.format("delta").load(str(table_path))
st.dataframe(df.toPandas(), use_container_width=True)
