import os
import requests
from google.cloud import storage
from google.cloud import bigquery

BUCKET_NAME = "jcdeah007-bucket"
FOLDER_NAME = "capstone3_rakhajidhan/raw"

PROJECT_ID = "jcdeah-007"
DATASET = "finalproject_rakhajidhan_ny_taxi_preparation"
TABLE = "green_tripdata"


def extract_and_upload(year, month):

    month = int(month)

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"

    file_name = f"green_tripdata_{year}-{month:02d}.parquet"

    print(f"Downloading {url}")

    r = requests.get(url)

    if r.status_code != 200:
        print(f"File not available: {url}")
        return

    with open(file_name, "wb") as f:
        f.write(r.content)

    print("Download completed")

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    blob = bucket.blob(f"{FOLDER_NAME}/{file_name}")

    blob.upload_from_filename(file_name)

    print(f"Uploaded to GCS: gs://{BUCKET_NAME}/{FOLDER_NAME}/{file_name}")

    os.remove(file_name)


def load_to_bigquery(year, month):

    month = int(month)

    uri = f"gs://{BUCKET_NAME}/{FOLDER_NAME}/green_tripdata_{year}-{month:02d}.parquet"

    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND"
    )

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()

    print("Loaded into BigQuery successfully")