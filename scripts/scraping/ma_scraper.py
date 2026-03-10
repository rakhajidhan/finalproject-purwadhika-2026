"""
ma_scraper.py
=============
All-in-one pipeline for Mahkamah Agung Putusan (Kategori: Agama).

Pipeline flow (all in this file):
    1. scrape_listing()     → scrape list pages for a given month
    2. scrape_detail()      → scrape detail fields for each putusan
    3. download_pdf()       → download each PDF → upload to GCS
    4. extract_pdf_text()   → extract structured text from PDF bytes
    5. scrape_list()        → main entry point called by Airflow DAG

Target months: November 2024, December 2024, January 2025, February 2025
GCS bucket   : jcdeah007-bucket
GCS folder   : finalproject_rakhajidhan/mahkamah_agung/pdf/
"""

import os
import re
import io
import time
import logging
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import pandas as pd
import pdfplumber
from datetime import datetime
from bs4 import BeautifulSoup
from google.cloud import storage

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

BASE_URL    = "https://putusan3.mahkamahagung.go.id"
BUCKET_NAME = "jcdeah007-bucket"
GCS_FOLDER  = "finalproject_rakhajidhan/mahkamah_agung/pdf"
LOCAL_TMP   = "/tmp/ma_pdfs"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# Months to process — used when Airflow calls scrape_list(year=2024)
# without a specific month, it processes all months for that year
BATCH_MONTHS = [
    {"year": 2024, "month": 11},
    {"year": 2024, "month": 12},
    {"year": 2025, "month":  1},
    {"year": 2025, "month":  2},
]


# ─────────────────────────────────────────────────────────────
# SECTION 1: HELPERS
# ─────────────────────────────────────────────────────────────

# Persistent session — verify=False is equivalent to curl -k
_session = requests.Session()
_session.verify = False
_session.headers.update(HEADERS)


def safe_get(url: str, params: dict = None, retries: int = 3, delay: float = 2.0) -> requests.Response:
    """HTTP GET with exponential retry. SSL disabled (same as curl -k)."""
    for attempt in range(1, retries + 1):
        try:
            resp = _session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.warning(f"[Attempt {attempt}] GET failed for {url}: {e}")
            if attempt < retries:
                time.sleep(delay * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {retries} retries")


def sanitize_filename(nomor: str) -> str:
    """Convert nomor putusan to a GCS-safe filename."""
    safe = re.sub(r"[^\w\-]", "_", nomor.strip())
    return safe + ".pdf"


def gcs_blob_exists(bucket, blob_name: str) -> bool:
    """Check if a GCS blob already exists — used for deduplication."""
    return bucket.blob(blob_name).exists()


# ─────────────────────────────────────────────────────────────
# SECTION 2: SCRAPE LISTING PAGES
# ─────────────────────────────────────────────────────────────

def get_listing_url(year: int, month: int, page: int = 1) -> str:
    """
    Build the correct direktori URL for Perdata Agama by year/month/page.
    Pattern: /direktori/index/kategori/perdata-agama-1/tahunjenis/putus/tahun/YYYY/bulan/MM/page/N.html
    """
    return (
        f"{BASE_URL}/direktori/index/kategori/perdata-agama-1"
        f"/tahunjenis/putus/tahun/{year}/bulan/{month:02d}/page/{page}.html"
    )


def get_total_pages(year: int, month: int) -> int:
    """Detect the total number of result pages for a given year/month."""
    url  = get_listing_url(year, month, page=1)
    resp = safe_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    pages = []
    for a in soup.select("ul.pagination li a"):
        try:
            pages.append(int(a.get_text(strip=True)))
        except ValueError:
            continue

    total = max(pages) if pages else 1
    logger.info(f"  [{year}-{month:02d}] Total pages: {total}")
    return total


def scrape_listing_page(year: int, month: int, page: int) -> list:
    """
    Scrape one listing page and return basic row info:
    nomor, judul, url_detail, tahun, bulan
    """
    url  = get_listing_url(year, month, page)
    resp = safe_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    items = soup.select("div.spost.clearfix") or soup.select("div.info-box")
    logger.info(f"  [{year}-{month:02d}] Page {page}: {len(items)} items found")

    rows = []
    for item in items:
        try:
            title_tag = item.select_one("h3 a") or item.select_one("a.entry-title")
            if not title_tag:
                continue

            judul      = title_tag.get_text(strip=True)
            detail_url = title_tag.get("href", "")
            if detail_url and not detail_url.startswith("http"):
                detail_url = BASE_URL + detail_url

            nomor = judul.split("Nomor")[-1].strip() if "Nomor" in judul else judul

            rows.append({
                "nomor":      nomor,
                "judul":      judul,
                "url_detail": detail_url,
                "tahun":      year,
                "bulan":      month,
            })
        except Exception as e:
            logger.warning(f"  Listing parse error: {e}")
            continue

    return rows


# ─────────────────────────────────────────────────────────────
# SECTION 3: SCRAPE DETAIL PAGE
# ─────────────────────────────────────────────────────────────

# Maps text labels on the detail page → DataFrame column names
DETAIL_LABEL_MAP = {
    "nomor":                   "nomor",
    "tingkat proses":          "tingkat_proses",
    "klasifikasi":             "klasifikasi",
    "kata kunci":              "kata_kunci",
    "tahun":                   "tahun_putusan",
    "tanggal register":        "tanggal_register",
    "lembaga peradilan":       "lembaga_peradilan",
    "jenis lembaga peradilan": "jenis_lembaga_peradilan",
    "hakim ketua":             "hakim_ketua",
    "hakim anggota":           "hakim_anggota",
    "panitera":                "panitera",
    "amar":                    "amar",
    "catatan amar":            "catatan_amar",
    "tanggal musyawarah":      "tanggal_musyawarah",
    "tanggal dibacakan":       "tanggal_dibacakan",
    "kaidah":                  "kaidah",
}


def scrape_detail(url_detail: str) -> dict:
    """
    Scrape structured fields from a putusan detail page.
    Also extracts the PDF download URL.
    Returns a dict of field → value.
    """
    if not url_detail:
        return {}

    try:
        resp = safe_get(url_detail)
    except RuntimeError as e:
        logger.error(f"  Detail page unreachable: {url_detail} — {e}")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    detail = {}

    # Parse label/value table rows
    for row in soup.select("table tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) >= 2:
            label_raw = cells[0].get_text(strip=True).lower()
            value     = cells[-1].get_text(" ", strip=True)
            for key, col in DETAIL_LABEL_MAP.items():
                if key in label_raw:
                    detail[col] = value
                    break

    # Extract PDF URL
    pdf_tag = soup.select_one("a[href*='.pdf']")
    if pdf_tag:
        href = pdf_tag.get("href", "")
        detail["pdf_url"] = href if href.startswith("http") else BASE_URL + href
    else:
        detail["pdf_url"] = None

    detail["url_detail"] = url_detail
    return detail


# ─────────────────────────────────────────────────────────────
# SECTION 4: DOWNLOAD PDF → UPLOAD TO GCS
# ─────────────────────────────────────────────────────────────

def download_pdf_bytes(pdf_url: str, retries: int = 3) -> bytes | None:
    """Download PDF from URL and return raw bytes."""
    for attempt in range(1, retries + 1):
        try:
            resp = _session.get(pdf_url, timeout=120, stream=True)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(f"  [Attempt {attempt}] PDF download failed {pdf_url}: {e}")
            if attempt < retries:
                time.sleep(2 * attempt)
    return None


def upload_pdf_to_gcs(pdf_bytes: bytes, blob_name: str, bucket) -> str:
    """Upload PDF bytes to GCS. Returns the gs:// URI."""
    blob = bucket.blob(blob_name)
    blob.upload_from_string(pdf_bytes, content_type="application/pdf")
    gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"
    logger.info(f"  Uploaded to GCS: {gcs_uri}")
    return gcs_uri


def process_pdf(nomor: str, pdf_url: str, bucket) -> dict:
    """
    Full PDF pipeline for one record:
      1. Check GCS dedup
      2. Download PDF bytes
      3. Upload to GCS
      4. Extract structured text from PDF
    Returns dict: {gcs_uri, pdf_pihak, pdf_isi_ringkas, pdf_dasar_hukum, pdf_amar_putusan}
    """
    result = {
        "gcs_uri":         None,
        "pdf_pihak":       None,
        "pdf_isi_ringkas": None,
        "pdf_dasar_hukum": None,
        "pdf_amar_putusan": None,
        "pdf_pages":       None,
    }

    if not pdf_url:
        return result

    filename  = sanitize_filename(nomor)
    blob_name = f"{GCS_FOLDER}/{filename}"

    # ── Dedup: if already in GCS, skip download ──
    if gcs_blob_exists(bucket, blob_name):
        result["gcs_uri"] = f"gs://{BUCKET_NAME}/{blob_name}"
        logger.info(f"  GCS dedup hit — skipping download for: {nomor}")
        # Still try to extract from existing blob
        try:
            blob  = bucket.blob(blob_name)
            pdf_bytes = blob.download_as_bytes()
            extracted = extract_pdf_text(pdf_bytes)
            result.update(extracted)
        except Exception as e:
            logger.warning(f"  Could not re-extract from GCS blob {blob_name}: {e}")
        return result

    # ── Download ──
    pdf_bytes = download_pdf_bytes(pdf_url)
    if not pdf_bytes:
        logger.error(f"  Could not download PDF for: {nomor}")
        return result

    # ── Upload to GCS ──
    try:
        gcs_uri = upload_pdf_to_gcs(pdf_bytes, blob_name, bucket)
        result["gcs_uri"] = gcs_uri
    except Exception as e:
        logger.error(f"  GCS upload failed for {nomor}: {e}")
        return result

    # ── Extract text ──
    extracted = extract_pdf_text(pdf_bytes)
    result.update(extracted)

    return result


# ─────────────────────────────────────────────────────────────
# SECTION 5: EXTRACT STRUCTURED TEXT FROM PDF
# ─────────────────────────────────────────────────────────────

def extract_pdf_text(pdf_bytes: bytes) -> dict:
    """
    Extract structured information from PDF bytes using pdfplumber.
    Targets common sections in Mahkamah Agung putusan PDFs:
      - Pihak (parties involved)
      - Isi ringkas (summary)
      - Dasar hukum (legal basis)
      - Amar putusan (decision/verdict)
    """
    extracted = {
        "pdf_pihak":        None,
        "pdf_isi_ringkas":  None,
        "pdf_dasar_hukum":  None,
        "pdf_amar_putusan": None,
        "pdf_pages":        None,
        "pdf_raw_text":     None,
    }

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            extracted["pdf_pages"] = len(pdf.pages)

            # Extract full text (first 10 pages to keep it manageable)
            full_text = ""
            for page in pdf.pages[:10]:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"

            if not full_text.strip():
                logger.warning("  PDF yielded no extractable text")
                return extracted

            # Store a truncated version of raw text (first 2000 chars)
            extracted["pdf_raw_text"] = full_text[:2000].strip()

            full_upper = full_text.upper()

            # ── Extract: Pihak (parties) ──
            pihak_match = re.search(
                r"(PEMOHON|PENGGUGAT|TERMOHON|TERGUGAT|PEMBANDING|TERBANDING)"
                r"(.*?)"
                r"(DUDUK PERKARA|TENTANG HUKUM|PERTIMBANGAN|MENGADILI|AMAR)",
                full_upper,
                re.DOTALL,
            )
            if pihak_match:
                extracted["pdf_pihak"] = full_text[
                    pihak_match.start():pihak_match.start() + 500
                ].strip()

            # ── Extract: Isi Ringkas / Duduk Perkara ──
            isi_match = re.search(
                r"(DUDUK PERKARA|TENTANG DUDUK PERKARA)(.*?)"
                r"(PERTIMBANGAN HUKUM|TENTANG HUKUM|MENGADILI)",
                full_upper,
                re.DOTALL,
            )
            if isi_match:
                start = isi_match.start()
                extracted["pdf_isi_ringkas"] = full_text[start:start + 800].strip()

            # ── Extract: Dasar Hukum ──
            hukum_match = re.search(
                r"(MEMPERHATIKAN|MENGINGAT)(.*?)(MENGADILI|AMAR PUTUSAN|M E N G A D I L I)",
                full_upper,
                re.DOTALL,
            )
            if hukum_match:
                start = hukum_match.start()
                extracted["pdf_dasar_hukum"] = full_text[start:start + 600].strip()

            # ── Extract: Amar Putusan ──
            amar_match = re.search(
                r"(MENGADILI|M E N G A D I L I|AMAR PUTUSAN)(.*?)($|\Z)",
                full_upper,
                re.DOTALL,
            )
            if amar_match:
                start = amar_match.start()
                extracted["pdf_amar_putusan"] = full_text[start:start + 600].strip()

    except Exception as e:
        logger.error(f"  PDF extraction error: {e}")

    return extracted


# ─────────────────────────────────────────────────────────────
# SECTION 6: MAIN ENTRY POINT (called by Airflow)
# ─────────────────────────────────────────────────────────────

def scrape_list(year: int, month: int = None) -> pd.DataFrame:
    """
    Main pipeline function — called by Airflow PythonOperator.

    For each month batch:
      1. Scrape listing pages
      2. Scrape detail for each putusan
      3. Download PDF → upload to GCS → extract text
    Returns a single merged DataFrame ready for BigQuery loading.

    Args:
        year  : Target year (e.g. 2024)
        month : Target month (1-12). If None, process all months in BATCH_MONTHS for that year.
    """
    # Determine batches to run
    if month:
        batches = [{"year": year, "month": month}]
    else:
        batches = [b for b in BATCH_MONTHS if b["year"] == year]

    if not batches:
        logger.warning(f"No configured batches found for year={year}, month={month}")
        return pd.DataFrame()

    # GCS client (shared across all batches)
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    os.makedirs(LOCAL_TMP, exist_ok=True)
    all_records = []

    for batch in batches:
        y, m = batch["year"], batch["month"]
        logger.info(f"\n{'='*50}")
        logger.info(f" Processing batch: {y}-{m:02d} (Agama)")
        logger.info(f"{'='*50}")

        try:
            total_pages = get_total_pages(y, m)
        except Exception as e:
            logger.error(f"  Cannot determine total pages for {y}-{m}: {e}")
            continue

        for page in range(1, total_pages + 1):
            logger.info(f"\n  --- Page {page}/{total_pages} ---")

            try:
                listing_rows = scrape_listing_page(y, m, page)
            except Exception as e:
                logger.error(f"  Listing scrape failed (page {page}): {e}")
                continue

            for row in listing_rows:
                nomor = row.get("nomor", "unknown")
                logger.info(f"  Processing: {nomor}")

                # ── Step 1: Scrape detail page ──
                time.sleep(0.5)
                try:
                    detail = scrape_detail(row["url_detail"])
                    row.update(detail)
                except Exception as e:
                    logger.warning(f"  Detail failed for {nomor}: {e}")

                # ── Step 2: PDF pipeline (download + upload + extract) ──
                pdf_url = row.get("pdf_url")
                if pdf_url:
                    time.sleep(0.5)
                    try:
                        pdf_result = process_pdf(nomor, pdf_url, bucket)
                        row.update(pdf_result)
                    except Exception as e:
                        logger.warning(f"  PDF pipeline failed for {nomor}: {e}")
                else:
                    logger.warning(f"  No PDF URL found for: {nomor}")

                # ── Step 3: Add metadata ──
                row["run_date"]   = datetime.utcnow().date().isoformat()
                row["kategori"]   = "Agama"
                row["scraped_at"] = datetime.utcnow().isoformat()

                all_records.append(row)

            time.sleep(1)  # polite delay between pages

    if not all_records:
        logger.warning("No records collected across all batches.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)

    # ── Global deduplication by nomor ──
    before = len(df)
    df = df.drop_duplicates(subset=["nomor"])
    after = len(df)
    logger.info(f"\nDeduplication: {before} → {after} records ({before - after} removed)")

    logger.info(f"Final DataFrame: {len(df)} rows, {len(df.columns)} columns")
    logger.info(f"Columns: {df.columns.tolist()}")

    return df


# ─────────────────────────────────────────────────────────────
# LOCAL TEST
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Test scraping November 2024 only
    df = scrape_list(year=2024, month=11)
    print(f"\nResult: {len(df)} records")
    print(df[["nomor", "tingkat_proses", "amar", "gcs_uri"]].head(5))