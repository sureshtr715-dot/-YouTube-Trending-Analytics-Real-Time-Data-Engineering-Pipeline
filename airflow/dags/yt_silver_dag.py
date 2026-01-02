from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ===============================
# Config (Environment-based)
# ===============================
PROJECT_DIR = "/opt/youtube-trending-analytics"
SPARK_SUBMIT = f"{PROJECT_DIR}/spark/bin/spark-submit"
SPARK_JOB = f"{PROJECT_DIR}/spark_jobs/yt_bronze_to_silver.py"

DELTA_PACKAGES = "io.delta:delta-spark_2.12:3.2.0"

# ===============================
# Default Args
# ===============================
default_args = {
    "owner": "name",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# ===============================
# DAG Definition
# ===============================
with DAG(
    dag_id="youtube_bronze_to_silver",
    description="Transform YouTube streaming data from Bronze to Silver layer",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["youtube", "delta-lake", "etl"],
) as dag:

    run_silver_job = BashOperator(
        task_id="run_bronze_to_silver_etl",
        bash_command=f"""
        export DATA_LAKE_PATH={PROJECT_DIR}/delta-lake && \
        {SPARK_SUBMIT} \
        --packages {DELTA_PACKAGES} \
        {SPARK_JOB}
        """,
    )

    run_silver_job
