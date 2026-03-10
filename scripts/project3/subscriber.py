import json
import logging
from datetime import datetime, timezone
from google.cloud import pubsub_v1, bigquery

# ===============================
# CONFIG
# ===============================

PROJECT_ID = "jcdeah-007"
SUBSCRIPTION_ID = "retail-transaction-rakhajidhan-sub"
DATASET_ID = "finalproject_rakhajidhan_pubsub_retail"
TABLE_ID = "retail_transactions_streaming"

# Max messages to pull per run (batch)
MAX_MESSAGES = 100
# Timeout for pulling (seconds)
ACK_DEADLINE = 30

logging.basicConfig(level=logging.INFO)


# ===============================
# TRANSFORM
# ===============================

def transform_message(raw_data: dict) -> dict:
    """
    Clean and enrich the raw Pub/Sub message before loading to BigQuery.
    """
    now = datetime.now(timezone.utc).isoformat()

    return {
        "transaction_id":   raw_data.get("transaction_id"),
        "event_type":       raw_data.get("event_type"),
        "customer_id":      int(raw_data.get("customer_id", 0)),
        "customer_name":    raw_data.get("customer_name"),
        "customer_city":    raw_data.get("customer_city"),
        "product_id":       int(raw_data.get("product_id", 0)),
        "product_name":     raw_data.get("product_name"),
        "product_category": raw_data.get("product_category"),
        "unit_price":       float(raw_data.get("unit_price", 0)),
        "quantity":         int(raw_data.get("quantity", 0)),
        "total_amount":     float(raw_data.get("total_amount", 0)),
        "payment_method":   raw_data.get("payment_method"),
        "status":           raw_data.get("status"),
        "published_at":     raw_data.get("published_at"),
        "ingested_at":      now,
        "run_date":         datetime.now(timezone.utc).date().isoformat(),
    }


# ===============================
# LOAD TO BIGQUERY
# ===============================

def load_to_bigquery(rows: list):
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    schema = [
        bigquery.SchemaField("transaction_id",   "STRING"),
        bigquery.SchemaField("event_type",        "STRING"),
        bigquery.SchemaField("customer_id",       "INTEGER"),
        bigquery.SchemaField("customer_name",     "STRING"),
        bigquery.SchemaField("customer_city",     "STRING"),
        bigquery.SchemaField("product_id",        "INTEGER"),
        bigquery.SchemaField("product_name",      "STRING"),
        bigquery.SchemaField("product_category",  "STRING"),
        bigquery.SchemaField("unit_price",        "FLOAT"),
        bigquery.SchemaField("quantity",          "INTEGER"),
        bigquery.SchemaField("total_amount",      "FLOAT"),
        bigquery.SchemaField("payment_method",    "STRING"),
        bigquery.SchemaField("status",            "STRING"),
        bigquery.SchemaField("published_at",      "STRING"),
        bigquery.SchemaField("ingested_at",       "STRING"),
        bigquery.SchemaField("run_date",          "DATE"),
    ]

    # Auto-create table with partition if not exists
    try:
        client.get_table(table_ref)
        logging.info(f"Table {table_ref} already exists.")
    except Exception:
        logging.info(f"Table not found. Creating {table_ref}...")
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date"
        )
        client.create_table(table)
        logging.info("Table created successfully.")

    errors = client.insert_rows_json(table_ref, rows)

    if errors:
        logging.error(f"BigQuery insert errors: {errors}")
        raise RuntimeError(f"Failed to insert {len(errors)} rows into BigQuery")
    else:
        logging.info(f"Successfully inserted {len(rows)} rows into {table_ref}")


# ===============================
# SUBSCRIBE & PROCESS
# ===============================

def subscribe_and_load():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    logging.info(f"Pulling messages from {subscription_path}")

    response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": MAX_MESSAGES,
        },
        timeout=ACK_DEADLINE,
    )

    received_messages = response.received_messages

    if not received_messages:
        logging.info("No messages received. Exiting.")
        return 0

    logging.info(f"Received {len(received_messages)} messages.")

    rows = []
    ack_ids = []

    for msg in received_messages:
        try:
            raw = json.loads(msg.message.data.decode("utf-8"))
            transformed = transform_message(raw)
            rows.append(transformed)
            ack_ids.append(msg.ack_id)
        except Exception as e:
            logging.warning(f"Failed to parse message {msg.message.message_id}: {e}")

    if rows:
        load_to_bigquery(rows)

    # Acknowledge successfully processed messages
    subscriber.acknowledge(
        request={
            "subscription": subscription_path,
            "ack_ids": ack_ids,
        }
    )

    logging.info(f"Acknowledged {len(ack_ids)} messages.")
    return len(rows)


if __name__ == "__main__":
    total = subscribe_and_load()
    logging.info(f"Subscriber finished. Total rows loaded: {total}")