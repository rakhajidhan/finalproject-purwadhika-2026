from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
import logging

# ===============================
# ALERT CONFIG
# ===============================

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1480905137110257880/AHlzWqXUlXEv9QeBmyKfmi93ZKJxEQO3MfnYZRhjvjlR3Ezcsqqyl2-GolR6Z15yZkOW"

# ===============================
# CALLBACK FUNCTIONS
# ===============================

def notify_discord(context, success: bool = False):
    import requests

    dag_id = context.get("dag").dag_id
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "N/A"
    execution_date = context.get("execution_date")
    log_url = task_instance.log_url if task_instance else "N/A"

    if success:
        color = 3066993  # Green
        title = f"✅ DAG Succeeded: {dag_id}"
        description = f"All tasks completed successfully.\n**Execution Date:** {execution_date}"
    else:
        color = 15158332  # Red
        title = f"❌ Task Failed: {task_id}"
        description = (
            f"**DAG:** {dag_id}\n"
            f"**Task:** {task_id}\n"
            f"**Execution Date:** {execution_date}\n"
            f"**Log URL:** {log_url}"
        )

    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": color,
            "footer": {"text": "Airflow Alert — Project 3 Pub/Sub Pipeline"},
        }]
    }

    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        r.raise_for_status()
        logging.info(f"Discord notification sent. Status: {r.status_code}")
    except Exception as e:
        logging.error(f"Failed to send Discord notification: {e}")


def on_failure_callback(context):
    notify_discord(context, success=False)


def on_success_callback(context):
    notify_discord(context, success=True)


def log_pipeline_start():
    logging.info("=" * 60)
    logging.info("Project 3 — Pub/Sub Retail Streaming Pipeline")
    logging.info(f"Start time: {datetime.utcnow().isoformat()}")
    logging.info("=" * 60)


def log_pipeline_end():
    logging.info("=" * 60)
    logging.info("Pipeline completed successfully.")
    logging.info(f"End time: {datetime.utcnow().isoformat()}")
    logging.info("=" * 60)


def send_success_notification(**context):
    """Dedicated task to send Discord success notification."""
    import requests

    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")

    payload = {
        "embeds": [{
            "title": f"✅ DAG Succeeded: {dag_id}",
            "description": f"All tasks completed successfully.\n**Execution Date:** {execution_date}",
            "color": 3066993,
            "footer": {"text": "Airflow Alert — Project 3 Pub/Sub Pipeline"},
        }]
    }

    r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    r.raise_for_status()
    logging.info(f"Discord success notification sent. Status: {r.status_code}")


# ===============================
# DEFAULT ARGS
# ===============================

default_args = {
    "owner": "rakha",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_callback,
}

# ===============================
# DAG DEFINITION
# ===============================

with DAG(
    dag_id="project3_pubsub_pipeline",
    default_args=default_args,
    description="Pub/Sub retail streaming pipeline — publish, subscribe, load to BigQuery",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["project3", "pubsub", "streaming", "bigquery"],
) as dag:

    start = PythonOperator(
        task_id="log_start",
        python_callable=log_pipeline_start,
    )

    run_publisher = BashOperator(
        task_id="run_publisher",
        bash_command="python /opt/airflow/scripts/project3/publisher.py",
    )

    run_subscriber = BashOperator(
        task_id="run_subscriber",
        bash_command="python /opt/airflow/scripts/project3/subscriber.py",
    )

    end = PythonOperator(
        task_id="log_end",
        python_callable=log_pipeline_end,
    )

    # Dedicated success notification task — more reliable than on_success_callback
    notify_success = PythonOperator(
        task_id="notify_discord_success",
        python_callable=send_success_notification,
        provide_context=True,
    )

    start >> run_publisher >> run_subscriber >> end >> notify_success