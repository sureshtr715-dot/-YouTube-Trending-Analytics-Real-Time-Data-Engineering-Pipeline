from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ===============================
# Config
# ===============================
PROJECT_DIR = "/opt/youtube-trending-analytics"
SPARK_SUBMIT = f"{PROJECT_DIR}/spark/bin/spark-submit"
SPARK_JOB = f"{PROJECT_DIR}/spark_jobs/yt_silver_to_gold.py"
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
    dag_id="youtube_silver_to_gold",
    description="Aggregate YouTube data from Silver to Gold analytics tables",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["youtube", "delta-lake", "analytics"],
) as dag:

    run_gold_job = BashOperator(
        task_id="run_silver_to_gold_etl",
        bash_command=f"""
        export DATA_LAKE_PATH={PROJECT_DIR}/delta-lake && \
        {SPARK_SUBMIT} \
        --packages {DELTA_PACKAGES} \
        {SPARK_JOB}
        """,
    )

    run_gold_job
