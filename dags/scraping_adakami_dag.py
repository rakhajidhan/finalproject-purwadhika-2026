from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
import logging
import sys
sys.path.append("/opt/airflow/scripts")

from scraping.scraping_adakami import scrape_adakami, load_to_bigquery # type: ignore



# ===============================
# DEFAULT ARGUMENTS
# ===============================
default_args = {
    "owner": "rakha",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ===============================
# DAG DEFINITION
# ===============================
with DAG(
    dag_id="scraping_adakami_daily",
    default_args=default_args,
    description="Daily scraping Adakami statistics API",
    schedule_interval="@daily",
    start_date=datetime(2026, 2, 20),
    catchup=False,
    tags=["scraping", "adakami", "api"],
) as dag:


    def run_scraping():
        logging.info("Starting Adakami scraping...")
        df = scrape_adakami()
        load_to_bigquery(df)

        logging.info("Scraping finished successfully.")
        logging.info(df)


    scraping_task = PythonOperator(
        task_id="scrape_adakami_api",
        python_callable=run_scraping,
    )


    scraping_task