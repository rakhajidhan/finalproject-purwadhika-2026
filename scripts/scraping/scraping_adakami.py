import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery



# ===============================
# CONFIGURATION
# ===============================
API_URL = "https://www.adakami.id/api/configuration/statistics"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ===============================
# SCRAPING FUNCTION
# ===============================
def scrape_adakami():
    """
    Scrape Adakami statistics API
    Return: pandas DataFrame
    """

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    try:
        response = requests.get(API_URL, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

    try:
        json_data = response.json()
    except ValueError:
        logging.error("Response is not valid JSON")
        raise

    # Validate API response
    if json_data.get("result") != 0:
        logging.error(f"API returned error: {json_data.get('resultMessage')}")
        raise Exception("API returned non-zero result")

    content = json_data.get("content")

    if not content:
        logging.error("No content found in API response")
        raise Exception("Empty content")

    # Convert numeric string to integer
    for key, value in content.items():
        if isinstance(value, str) and value.isdigit():
            content[key] = int(value)

    # Add scraping timestamp (proper datetime type)
    content["scraping_date"] = pd.Timestamp.now()

    # Create DataFrame
    df = pd.DataFrame([content])

    logging.info("Scraping successful")
    logging.info(df)

    return df
#==================
# LOAD TO BIG QUERY
#==================
def load_to_bigquery(df):

    client = bigquery.Client(project="jcdeah-007")

    table_id = "jcdeah-007.finalproject_rakhajidhan_adakami.prep_adakami_statistics"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

    job.result()

    print("Loaded to BigQuery successfully.")
# ===============================
# LOCAL TESTING
# ===============================
if __name__ == "__main__":
    df = scrape_adakami()

    print("\n=== DATA ===")
    print(df)

    print("\n=== DATATYPES ===")
    print(df.dtypes)