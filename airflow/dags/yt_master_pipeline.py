from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# ===============================
# Default Args
# ===============================
default_args = {
    "owner": "name",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# ===============================
# Master DAG
# ===============================
with DAG(
    dag_id="youtube_master_pipeline",
    description="Master DAG orchestrating Bronze → Silver → Gold YouTube analytics pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["youtube", "etl", "delta-lake", "airflow"],
) as dag:

    # ---------------------------
    # Bronze → Silver
    # ---------------------------
    run_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="yt_bronze_to_silver",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    # ---------------------------
    # Silver → Gold
    # ---------------------------
    run_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="yt_silver_to_gold",
        wait_for_completion=True,
        reset_dag_run=True,
    )

    run_silver >> run_gold
