import logging
import requests
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.models.param import Param

import sys
sys.path.append("/opt/airflow/scripts")

from scraping.ma_scraper import scrape_list          # type: ignore
from load.ma_bigquery_loader import load_to_bigquery  # type: ignore

logger = logging.getLogger(__name__)


DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1480950650014531740/UCX_Ykp2nI0C5OdrNvFEPEdQreFB214wBNNHgi-0Cyl-uRj20IA0RIp3_k1ennpupBte"


def send_discord_alert(context: dict):
    """Sends a Discord notification when a task fails."""
    dag_id  = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    run_id  = context.get("run_id", "unknown")
    log_url = context.get("task_instance").log_url
    exec_dt = context.get("execution_date")

    message = {
        "embeds": [{
            "title":       "❌ Airflow Task Failed",
            "color":       15158332,
            "description": (
                f"**DAG**    : `{dag_id}`\n"
                f"**Task**   : `{task_id}`\n"
                f"**Run ID** : `{run_id}`\n"
                f"**Date**   : `{exec_dt}`\n"
                f"[🔗 View Logs]({log_url})"
            ),
            "footer":    {"text": "Mahkamah Agung Pipeline"},
            "timestamp": datetime.utcnow().isoformat(),
        }]
    }

    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=10)
        resp.raise_for_status()
        logger.info(f"Discord alert sent for task: {task_id}")
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")


# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────────────────────

def task_scrape_and_extract(**context):
    """
    Task 1: Scrape front-page listing + detail + PDF for the given year.
    Reads year from DAG params at trigger time.
    Pushes result DataFrame as JSON into XCom.
    """
    year = context["params"]["year"]

    logger.info(f"Starting front-page scrape: year={year} (all categories)")

    df = scrape_list(year=year)

    if df.empty:
        logger.warning(f"No data scraped for year={year}")
        context["ti"].xcom_push(key="scraped_data", value=None)
        return

    logger.info(f"Scraped {len(df)} records for year={year}")
    context["ti"].xcom_push(
        key="scraped_data",
        value=df.to_json(orient="records", date_format="iso"),
    )


def task_load_to_bigquery(**context):
    """
    Task 2: Pull DataFrame from XCom and load into BigQuery.
    """
    raw_json = context["ti"].xcom_pull(key="scraped_data")

    if not raw_json:
        logger.warning("No data in XCom — skipping BQ load.")
        return

    df = pd.read_json(raw_json, orient="records")

    if df.empty:
        logger.warning("DataFrame is empty — skipping BQ load.")
        return

    logger.info(f"Loading {len(df)} records to BigQuery...")
    load_to_bigquery(df)
    logger.info("BigQuery load complete.")


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner":               "rakha",
    "retries":             2,
    "retry_delay":         timedelta(minutes=5),
    "on_failure_callback": send_discord_alert,
}

with DAG(
    dag_id="mahkamah_agung_pipeline",
    default_args=default_args,
    description="Scrape Mahkamah Agung putusan front page — triggered manually per year",
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mahkamah_agung", "scraping", "final_project"],
    params={
        "year": Param(2026, type="integer", description="Year to scrape (e.g. 2026)"),
    },
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_and_extract",
        python_callable=task_scrape_and_extract,
        execution_timeout=timedelta(hours=3),
    )

    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=task_load_to_bigquery,
        execution_timeout=timedelta(minutes=30),
    )

    scrape_task >> load_task