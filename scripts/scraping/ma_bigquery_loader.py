import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "https://putusan3.mahkamahagung.go.id"
DOWNLOAD_FOLDER = "data/pdf"

# ==============================
# 1. CREATE STABLE SESSION
# ==============================

def get_session():
    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    })

    return session


# ==============================
# 2. SCRAPE LIST PAGE
# ==============================

def scrape_list_page(session, year=2024):
    url = f"{BASE_URL}/direktori/index/tahunjenis/putus/tahun/{year}.html"

    print(f"\nScraping list page for year {year}")
    response = session.get(url, timeout=60)
    print("Status:", response.status_code)

    soup = BeautifulSoup(response.text, "html.parser")

    detail_links = []

    for a in soup.find_all("a"):
        href = a.get("href")
        text = a.get_text(strip=True)

        if href and "/direktori/putusan/" in href:
            full_url = href if href.startswith("http") else BASE_URL + href
            detail_links.append((text, full_url))

    print("Total detail links found:", len(detail_links))

    return detail_links


# ==============================
# 3. SCRAPE DETAIL PAGE
# ==============================

def scrape_detail_page(session, url):
    print(f"\nScraping detail: {url}")

    response = session.get(url, timeout=60)
    soup = BeautifulSoup(response.text, "html.parser")

    metadata = {}

    rows = soup.find_all("tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) == 2:
            key = cols[0].get_text(strip=True)
            value = cols[1].get_text(strip=True)
            metadata[key] = value

    # Find PDF link
    pdf_link = None
    for a in soup.find_all("a"):
        href = a.get("href")
        if href and ".pdf" in href:
            pdf_link = href if href.startswith("http") else BASE_URL + href
            break

    return metadata, pdf_link


# ==============================
# 4. DOWNLOAD PDF
# ==============================

def download_pdf(session, pdf_url, filename):
    if not pdf_url:
        return None

    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

    file_path = os.path.join(DOWNLOAD_FOLDER, filename)

    print(f"Downloading PDF: {filename}")

    response = session.get(pdf_url, timeout=60)

    with open(file_path, "wb") as f:
        f.write(response.content)

    return file_path


# ==============================
# 5. MAIN EXECUTION
# ==============================

def main():
    session = get_session()

    year = 2024
    detail_links = scrape_list_page(session, year)

    all_data = []

    # Batasi 5 saja dulu untuk test
    for title, detail_url in detail_links[:5]:

        metadata, pdf_link = scrape_detail_page(session, detail_url)

        filename = None
        if pdf_link:
            filename = pdf_link.split("/")[-1]
            download_pdf(session, pdf_link, filename)

        record = {
            "title": title,
            "detail_url": detail_url,
            "pdf_url": pdf_link
        }

        # merge metadata
        record.update(metadata)

        all_data.append(record)

        # delay supaya tidak hammer server
        time.sleep(2)

    df = pd.DataFrame(all_data)

    print("\n=== SAMPLE DATA ===")
    print(df.head())

    df.to_csv("ma_sample_output.csv", index=False)
    print("\nSaved to ma_sample_output.csv")


if __name__ == "__main__":
    main()