"""
ma_bigquery_loader.py
=====================
Loads scraped Mahkamah Agung putusan data into BigQuery.

Two tables are loaded:
  1. putusan_list       — listing + detail fields (one row per putusan)
  2. putusan_pdf_detail — extracted PDF text fields (one row per putusan)

Both tables use:
  - Partitioning by run_date
  - WRITE_APPEND with dedup via merge (no duplicate nomor per run_date)
  - Dataset: finalproject_rakhajidhan_mahkamahagung
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

PROJECT_ID = "jcdeah-007"
DATASET_ID = "finalproject_rakhajidhan_mahkamahagung"


# ─────────────────────────────────────────────────────────────
# BIGQUERY SCHEMAS
# ─────────────────────────────────────────────────────────────

SCHEMA_PUTUSAN_LIST = [
    bigquery.SchemaField("nomor",                    "STRING"),
    bigquery.SchemaField("judul",                    "STRING"),
    bigquery.SchemaField("url_detail",               "STRING"),
    bigquery.SchemaField("tahun",                    "INTEGER"),
    bigquery.SchemaField("bulan",                    "INTEGER"),
    bigquery.SchemaField("tingkat_proses",           "STRING"),
    bigquery.SchemaField("klasifikasi",              "STRING"),
    bigquery.SchemaField("kata_kunci",               "STRING"),
    bigquery.SchemaField("tahun_putusan",            "STRING"),
    bigquery.SchemaField("tanggal_register",         "STRING"),
    bigquery.SchemaField("lembaga_peradilan",        "STRING"),
    bigquery.SchemaField("jenis_lembaga_peradilan",  "STRING"),
    bigquery.SchemaField("hakim_ketua",              "STRING"),
    bigquery.SchemaField("hakim_anggota",            "STRING"),
    bigquery.SchemaField("panitera",                 "STRING"),
    bigquery.SchemaField("amar",                     "STRING"),
    bigquery.SchemaField("catatan_amar",             "STRING"),
    bigquery.SchemaField("tanggal_musyawarah",       "STRING"),
    bigquery.SchemaField("tanggal_dibacakan",        "STRING"),
    bigquery.SchemaField("kaidah",                   "STRING"),
    bigquery.SchemaField("pdf_url",                  "STRING"),
    bigquery.SchemaField("gcs_uri",                  "STRING"),
    bigquery.SchemaField("kategori",                 "STRING"),
    bigquery.SchemaField("scraped_at",               "TIMESTAMP"),
    bigquery.SchemaField("run_date",                 "DATE"),
]

SCHEMA_PDF_DETAIL = [
    bigquery.SchemaField("nomor",             "STRING"),
    bigquery.SchemaField("gcs_uri",           "STRING"),
    bigquery.SchemaField("pdf_pages",         "INTEGER"),
    bigquery.SchemaField("pdf_pihak",         "STRING"),
    bigquery.SchemaField("pdf_isi_ringkas",   "STRING"),
    bigquery.SchemaField("pdf_dasar_hukum",   "STRING"),
    bigquery.SchemaField("pdf_amar_putusan",  "STRING"),
    bigquery.SchemaField("pdf_raw_text",      "STRING"),
    bigquery.SchemaField("run_date",          "DATE"),
]


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def ensure_dataset_exists(client: bigquery.Client):
    """Create BQ dataset if it doesn't exist yet."""
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset already exists: {DATASET_ID}")
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)
        logger.info(f"Created dataset: {DATASET_ID}")


def table_exists(client: bigquery.Client, table_id: str) -> bool:
    """Check if a BQ table exists."""
    try:
        client.get_table(table_id)
        return True
    except Exception:
        return False


def create_table(client: bigquery.Client, table_id: str, schema: list):
    """Create a partitioned BQ table."""
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="run_date",
    )
    client.create_table(table, exists_ok=True)
    logger.info(f"Table ready: {table_id}")


def dedup_against_bq(client: bigquery.Client, df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    """
    Remove from df any nomor values already present in BigQuery for the same run_date.
    This prevents duplicates on re-runs.
    """
    if df.empty:
        return df

    if not table_exists(client, table_id):
        return df  # Table doesn't exist yet, nothing to dedup against

    run_date = df["run_date"].iloc[0]

    query = f"""
        SELECT DISTINCT nomor
        FROM `{table_id}`
        WHERE run_date = '{run_date}'
    """
    try:
        existing = client.query(query).to_dataframe()
        existing_nomors = set(existing["nomor"].tolist())
        before = len(df)
        df = df[~df["nomor"].isin(existing_nomors)]
        after = len(df)
        logger.info(f"  BQ dedup [{table_id.split('.')[-1]}]: {before} → {after} rows ({before - after} skipped)")
    except Exception as e:
        logger.warning(f"  BQ dedup check failed (will proceed without it): {e}")

    return df


def prepare_df_for_table(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """
    Select and cast only columns that exist in the schema.
    Missing columns are filled with None.
    """
    schema_cols = [f.name for f in schema]
    for col in schema_cols:
        if col not in df.columns:
            df[col] = None

    df = df[schema_cols].copy()

    # Cast types
    for field in schema:
        col = field.name
        if field.field_type == "INTEGER":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif field.field_type == "TIMESTAMP":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif field.field_type == "DATE":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        else:
            df[col] = df[col].astype(str).replace("nan", None).replace("None", None)

    return df


# ─────────────────────────────────────────────────────────────
# MAIN LOAD FUNCTION
# ─────────────────────────────────────────────────────────────

def load_to_bigquery(df: pd.DataFrame):
    """
    Load the scraped DataFrame into two BigQuery tables:
      - putusan_list       (all listing + detail fields)
      - putusan_pdf_detail (PDF-extracted fields only)

    Args:
        df: DataFrame returned by ma_scraper.scrape_list()
    """
    if df is None or df.empty:
        logger.warning("Empty DataFrame — nothing to load into BigQuery.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    ensure_dataset_exists(client)

    table_list_id = f"{PROJECT_ID}.{DATASET_ID}.putusan_list"
    table_pdf_id  = f"{PROJECT_ID}.{DATASET_ID}.putusan_pdf_detail"

    # Ensure tables exist
    create_table(client, table_list_id, SCHEMA_PUTUSAN_LIST)
    create_table(client, table_pdf_id,  SCHEMA_PDF_DETAIL)

    # ── Load 1: putusan_list ──
    logger.info("\n--- Loading: putusan_list ---")
    df_list = prepare_df_for_table(df.copy(), SCHEMA_PUTUSAN_LIST)
    df_list = dedup_against_bq(client, df_list, table_list_id)

    if not df_list.empty:
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA_PUTUSAN_LIST,
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="run_date",
            ),
        )
        job = client.load_table_from_dataframe(df_list, table_list_id, job_config=job_config)
        job.result()
        logger.info(f"  Loaded {len(df_list)} rows into {table_list_id}")
    else:
        logger.info("  No new rows to insert into putusan_list (all already exist).")

    # ── Load 2: putusan_pdf_detail ──
    logger.info("\n--- Loading: putusan_pdf_detail ---")

    # Only include rows that have PDF data
    pdf_cols_present = [c for c in df.columns if c.startswith("pdf_") or c in ("nomor", "gcs_uri", "run_date")]
    df_pdf = df[pdf_cols_present].copy() if pdf_cols_present else pd.DataFrame()

    if not df_pdf.empty:
        df_pdf = prepare_df_for_table(df_pdf, SCHEMA_PDF_DETAIL)
        df_pdf = dedup_against_bq(client, df_pdf, table_pdf_id)

        if not df_pdf.empty:
            job_config_pdf = bigquery.LoadJobConfig(
                schema=SCHEMA_PDF_DETAIL,
                write_disposition="WRITE_APPEND",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="run_date",
                ),
            )
            job = client.load_table_from_dataframe(df_pdf, table_pdf_id, job_config=job_config_pdf)
            job.result()
            logger.info(f"  Loaded {len(df_pdf)} rows into {table_pdf_id}")
        else:
            logger.info("  No new rows to insert into putusan_pdf_detail (all already exist).")
    else:
        logger.info("  No PDF detail columns found in DataFrame — skipping putusan_pdf_detail load.")

    logger.info("\n✅ BigQuery load complete.")


if __name__ == "__main__":
    # Quick test with dummy data
    dummy_df = pd.DataFrame([{
        "nomor":           "6578 K/PID.SUS/2025",
        "judul":           "Putusan Mahkamah Agung Nomor 6578 K/PID.SUS/2025",
        "kategori":        "Agama",
        "tahun":           2025,
        "bulan":           8,
        "amar":            "Tolak",
        "gcs_uri":         "gs://jcdeah007-bucket/finalproject_rakhajidhan/mahkamah_agung/pdf/test.pdf",
        "pdf_pages":       12,
        "pdf_pihak":       "Penggugat vs Tergugat",
        "pdf_amar_putusan": "Menolak permohonan kasasi",
        "run_date":        datetime.utcnow().date().isoformat(),
        "scraped_at":      datetime.utcnow().isoformat(),
    }])
    load_to_bigquery(dummy_df)