from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from google.cloud import bigquery
import pandas as pd


# -------------------------------
# CONFIG
# Matches docker-compose.yml:
#   host: postgres (service name)
#   port: 5432 (internal)
#   user/password: airflow/airflow
#   database: retail_db
# -------------------------------

PROJECT_ID = "jcdeah-007"
DATASET_ID = "finalproject_rakhajidhan_ecommerce_retails"
TABLES     = ["customers", "products", "purchase"]

PG_CONFIG = {
    "host":     "postgres",
    "port":     5432,
    "database": "retail_db",
    "user":     "airflow",
    "password": "airflow",
}


# -------------------------------
# ETL FUNCTION
# -------------------------------

def extract_transform_load(table_name: str, **context):
    today     = datetime.now().date()
    yesterday = today - timedelta(days=1)

    print(f"[{table_name}] Starting ETL for run_date={yesterday}")

    pg_conn   = psycopg2.connect(**PG_CONFIG)
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_id  = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    # ---- Check if BQ table already exists (initial vs incremental) ----
    try:
        bq_client.get_table(table_id)
        table_exists = True
        print(f"[{table_name}] Table exists → incremental load (H-1: {yesterday})")
    except Exception:
        table_exists = False
        print(f"[{table_name}] Table not found → full initial load")

    # ---- Query Postgres ----
    if not table_exists:
        query = f"SELECT *, DATE(created_at) AS run_date FROM {table_name}"
        df = pd.read_sql(query, pg_conn)
    else:
        query = f"""
            SELECT *, DATE(created_at) AS run_date
            FROM {table_name}
            WHERE DATE(created_at) = %s
        """
        df = pd.read_sql(query, pg_conn, params=[yesterday])

    pg_conn.close()

    if df.empty:
        print(f"[{table_name}] No data found — skipping load.")
        return

    print(f"[{table_name}] Fetched {len(df)} rows from Postgres")

    # ---- Load to BigQuery (partitioned by run_date) ----
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date",
        ),
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"[{table_name}] ✓ Loaded {len(df)} rows → {table_id}")


# -------------------------------
# AIRFLOW DAG
# -------------------------------

default_args = {
    "owner": "rakha",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="project1_postgres_to_bigquery_daily_incremental",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["project1", "retail", "bigquery"],
    description="Daily incremental load from PostgreSQL retail_db to BigQuery",
) as dag:

    tasks = []
    for table in TABLES:
        task = PythonOperator(
            task_id=f"load_{table}_to_bq",
            python_callable=extract_transform_load,
            op_kwargs={"table_name": table},
        )
        tasks.append(task)

    # Sequential: customers → products → purchase
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]