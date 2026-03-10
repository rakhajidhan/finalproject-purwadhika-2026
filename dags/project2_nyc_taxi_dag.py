from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/scripts/project2")
from green_taxi_pipeline import process_month


'''# ─── ALERT CALLBACK ───────────────────────────────────────
def on_failure_callback(context):
    import requests as req

    dag_id  = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    log_url = context.get("task_instance").log_url
    exc     = context.get("exception")

    message = (
        f"🚨 **Airflow Task Failed**\n"
        f"**DAG:** `{dag_id}`\n"
        f"**Task:** `{task_id}`\n"
        f"**Error:** `{exc}`\n"
        f"**Log:** {log_url}"
    )

    DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/YOUR_WEBHOOK_HERE"
    try:
        req.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10)
        print("[ALERT] Failure notification sent to Discord.")
    except Exception as e:
        print(f"[ALERT] Failed to send Discord notification: {e}")'''


# ─── DEFAULT ARGS ─────────────────────────────────────────
default_args = {
    "owner"              : "rakha",
    "retries"            : 1,
    "retry_delay"        : timedelta(seconds=10),
    #"on_failure_callback": on_failure_callback,
}

# ─── DAG ──────────────────────────────────────────────────
with DAG(
    dag_id           = "project2_green_taxi_pipeline",
    default_args     = default_args,
    description      = "NYC Green Taxi ETL — GCS staging → BigQuery",
    start_date       = datetime(2023, 1, 1),
    schedule_interval= "@monthly",
    catchup          = False,
    tags             = ["project2", "nyc-taxi", "green"],
) as dag:

    run_pipeline = PythonOperator(
        task_id         = "process_green_taxi_month",
        python_callable = process_month,
    )