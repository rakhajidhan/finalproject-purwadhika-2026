import os
import requests
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


# ─── CONFIG ───────────────────────────────────────────────
PROJECT_ID       = "jcdeah-007"
DATASET          = "finalproject_rakhajidhan_ny_taxi_preparation"
DATASET_GREENTAXI = "finalproject_rakhajidhan_greentaxi_preparation"
TABLE            = "green_tripdata"
BUCKET_NAME      = "jcdeah007-bucket"
FOLDER           = "capstone3_rakhajidhan/raw"
BASE_URL         = "https://d37ci6vzurychx.cloudfront.net/trip-data"


# ─── HELPERS ──────────────────────────────────────────────

def ensure_dataset(bq_client: bigquery.Client, dataset_id: str) -> None:
    """Create BQ dataset if it doesn't exist yet."""
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
    dataset_ref.location = "US"
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    print(f"  [BQ] Dataset ready: {dataset_id}")


def dataset_exists(year: int, month: int) -> bool:
    """
    Check availability using GET with stream=True instead of HEAD.
    CloudFront sometimes blocks HEAD requests, causing false 404s.
    We only download the first few bytes to confirm the file exists.
    """
    url = f"{BASE_URL}/green_tripdata_{year}-{month:02d}.parquet"
    try:
        r = requests.get(url, stream=True, timeout=15)
        r.close()
        return r.status_code == 200
    except Exception as e:
        print(f"  [WARN] Availability check failed for {year}-{month:02d}: {e}")
        return False


def drop_bq_table(bq_client: bigquery.Client, table_id: str) -> None:
    try:
        bq_client.delete_table(table_id)
        print(f"  [BQ] Table '{table_id}' dropped — will be recreated fresh.")
    except NotFound:
        print(f"  [BQ] Table '{table_id}' not found — will be created fresh.")


# ─── MAIN PIPELINE ────────────────────────────────────────

def process_month() -> None:
    run_date = date.today().isoformat()
    current  = datetime(2023, 1, 1)

    storage_client = storage.Client()
    bucket         = storage_client.bucket(BUCKET_NAME)
    bq_client      = bigquery.Client()

    # ── Ensure both BQ datasets exist before any operations ──
    ensure_dataset(bq_client, DATASET)
    ensure_dataset(bq_client, DATASET_GREENTAXI)

    table_id         = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    staging_table_id = f"{PROJECT_ID}.{DATASET_GREENTAXI}.{TABLE}_staging"

    drop_bq_table(bq_client, table_id)
    first_load = True

    while True:
        year  = current.year
        month = current.month
        tag   = f"{year}-{month:02d}"

        print(f"\n{'='*50}")
        print(f"  Processing: {tag}")
        print(f"{'='*50}")

        # ── 1. Check availability ──────────────────────────
        print(f"  [CHECK] Verifying dataset availability ...")
        if not dataset_exists(year, month):
            print(f"  [STOP] No data for {tag} — ingestion complete.")
            break

        # ── 2. Download ────────────────────────────────────
        url      = f"{BASE_URL}/green_tripdata_{tag}.parquet"
        filename = f"green_tripdata_{tag}.parquet"

        print(f"  [DOWNLOAD] {url}")
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        with open(filename, "wb") as f:
            f.write(r.content)
        print(f"  [DOWNLOAD] Complete ({len(r.content) / 1_000_000:.1f} MB)")

        # ── 3. Upload to GCS ───────────────────────────────
        gcs_path = f"{FOLDER}/{filename}"
        blob     = bucket.blob(gcs_path)
        blob.upload_from_filename(filename)
        print(f"  [GCS] Uploaded → gs://{BUCKET_NAME}/{gcs_path}")

        # ── 4. Load to staging with AUTODETECT ────────────
        uri = f"gs://{BUCKET_NAME}/{gcs_path}"

        staging_config = bigquery.LoadJobConfig(
            source_format     = bigquery.SourceFormat.PARQUET,
            write_disposition = "WRITE_TRUNCATE",
            autodetect        = True,
        )

        print(f"  [BQ] Loading into staging: {staging_table_id} ...")
        staging_job = bq_client.load_table_from_uri(
            uri, staging_table_id, job_config=staging_config
        )
        staging_job.result()
        print(f"  [BQ] Staging load complete.")

        # ── 5. Insert from staging → main with type casting ──
        if first_load:
            sql = f"""
                CREATE OR REPLACE TABLE `{table_id}`
                PARTITION BY run_date
                AS
                SELECT
                    VendorID,
                    lpep_pickup_datetime,
                    lpep_dropoff_datetime,
                    store_and_fwd_flag,
                    CAST(RatecodeID AS FLOAT64)            AS RatecodeID,
                    PULocationID,
                    DOLocationID,
                    CAST(passenger_count AS FLOAT64)       AS passenger_count,
                    CAST(trip_distance AS FLOAT64)         AS trip_distance,
                    CAST(fare_amount AS FLOAT64)           AS fare_amount,
                    CAST(extra AS FLOAT64)                 AS extra,
                    CAST(mta_tax AS FLOAT64)               AS mta_tax,
                    CAST(tip_amount AS FLOAT64)            AS tip_amount,
                    CAST(tolls_amount AS FLOAT64)          AS tolls_amount,
                    CAST(ehail_fee AS FLOAT64)             AS ehail_fee,
                    CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
                    CAST(total_amount AS FLOAT64)          AS total_amount,
                    CAST(payment_type AS FLOAT64)          AS payment_type,
                    CAST(trip_type AS FLOAT64)             AS trip_type,
                    CAST(congestion_surcharge AS FLOAT64)  AS congestion_surcharge,
                    DATE('{run_date}')                     AS run_date
                FROM `{staging_table_id}`
            """
        else:
            sql = f"""
                INSERT INTO `{table_id}`
                SELECT
                    VendorID,
                    lpep_pickup_datetime,
                    lpep_dropoff_datetime,
                    store_and_fwd_flag,
                    CAST(RatecodeID AS FLOAT64)            AS RatecodeID,
                    PULocationID,
                    DOLocationID,
                    CAST(passenger_count AS FLOAT64)       AS passenger_count,
                    CAST(trip_distance AS FLOAT64)         AS trip_distance,
                    CAST(fare_amount AS FLOAT64)           AS fare_amount,
                    CAST(extra AS FLOAT64)                 AS extra,
                    CAST(mta_tax AS FLOAT64)               AS mta_tax,
                    CAST(tip_amount AS FLOAT64)            AS tip_amount,
                    CAST(tolls_amount AS FLOAT64)          AS tolls_amount,
                    CAST(ehail_fee AS FLOAT64)             AS ehail_fee,
                    CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
                    CAST(total_amount AS FLOAT64)          AS total_amount,
                    CAST(payment_type AS FLOAT64)          AS payment_type,
                    CAST(trip_type AS FLOAT64)             AS trip_type,
                    CAST(congestion_surcharge AS FLOAT64)  AS congestion_surcharge,
                    DATE('{run_date}')                     AS run_date
                FROM `{staging_table_id}`
            """

        print(f"  [BQ] Inserting into main table with type casting ...")
        query_job = bq_client.query(sql)
        query_job.result()
        first_load = False
        print(f"  [BQ] Insert complete.")

        # ── 6. Cleanup local file ──────────────────────────
        if os.path.exists(filename):
            os.remove(filename)
        print(f"  [CLEANUP] Local file removed.")

        current += relativedelta(months=1)

    # Drop staging table after all months done
    try:
        bq_client.delete_table(staging_table_id)
        print(f"\n  [BQ] Staging table cleaned up.")
    except Exception:
        pass

    print(f"\n[DONE] Green taxi pipeline finished. run_date={run_date}")