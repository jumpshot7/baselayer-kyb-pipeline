"""
fetcher.py

Downloads datasets from Socrata and streams them to Google Cloud Storage.

NYC DCA dataset: bulk export (already NYC-only by definition, no filter needed)
NYS corporations: paginated /resource/ API with two optimizations:
  1. $where  -> county filter, only NYC boroughs (New York, Kings, Queens,
                Bronx, Richmond) — cuts ~4M nationwide rows to ~200-400K
  2. $select -> only the 4 columns we actually use in the pipeline:
                dos_id, current_entity_name, initial_dos_filing_date,
                dos_process_zip
                All other columns (CEO address, registered agent, location,
                entity_type, jurisdiction, etc.) are excluded to minimize
                storage usage on the free database tier.

Why paginated instead of bulk export?
The bulk export endpoint (/rows.csv?accessType=DOWNLOAD) ignores SoQL
$where and $select parameters — you always get the full dataset with all
columns. The /resource/ endpoint supports both, but caps at 50,000 rows
per request, so we paginate until we get an empty page.
"""

import csv
import io
import logging
import os
import time
import tempfile
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NYC_DOMAIN = "data.cityofnewyork.us"
NYC_DATASET = "w7w3-xahh"

NYS_DOMAIN = "data.ny.gov"
NYS_DATASET = "n9v6-gdp6"

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

# Title case — must match exactly how values appear in the Socrata dataset
NYC_COUNTIES = ("New York", "Kings", "Queens", "Bronx", "Richmond")

# Only fetch the 4 columns we actually use in the pipeline.
# Everything else is excluded to save storage.
NYS_COLUMNS = ",".join([
    "dos_id",
    "current_entity_name",
    "initial_dos_filing_date",
    "dos_process_zip",
])

PAGE_SIZE = 50000
MAX_PAGE_RETRIES = 5
REQUEST_TIMEOUT = 120


def make_session() -> requests.Session:
    """
    Creates a requests Session with automatic retry on connection errors.
    HTTPAdapter + Retry handles transient network failures automatically.
    backoff_factor=2 means waits of 2s, 4s, 8s between retries.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ============================================================
# Bulk Download (NYC DCA — no filter needed)
# ============================================================

def stream_bulk_to_gcs(domain: str, dataset_id: str, blob_name: str) -> str:
    """
    Downloads a full Socrata dataset via bulk CSV export and uploads to GCS.
    Used for NYC DCA businesses which are already NYC-only by definition.
    """
    url = f"https://{domain}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"
    logger.info(f"[fetcher] Bulk download: {dataset_id} from {domain}")

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)

    headers = {"Accept-Encoding": "identity"}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    session = make_session()

    with session.get(url, stream=True, headers=headers, timeout=REQUEST_TIMEOUT) as response:
        response.raise_for_status()

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_file:
            bytes_downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    tmp_file.write(chunk)
                    bytes_downloaded += len(chunk)
                    if bytes_downloaded % (10 * 1024 * 1024) < len(chunk):
                        logger.info(
                            f"[fetcher] {dataset_id}: {bytes_downloaded // (1024 * 1024)} MB downloaded"
                        )

            tmp_file.flush()
            logger.info(f"[fetcher] Download complete ({bytes_downloaded} bytes). Uploading to GCS...")
            blob.upload_from_filename(tmp_file.name, content_type="text/csv")

    gcs_url = f"gs://{GCS_BUCKET_NAME}/{blob_name}"
    logger.info(f"[fetcher] Uploaded {dataset_id} -> {gcs_url}")
    return gcs_url


# ============================================================
# Paginated Download (NYS corporations — filtered + column-pruned)
# ============================================================

def fetch_page_with_retry(session, base_url, params, headers, page) -> requests.Response:
    """
    Fetches a single page from the Socrata API with manual retry logic
    on top of the session-level retries. Handles Timeout specifically
    with longer waits since timeouts often mean server load.
    """
    for attempt in range(MAX_PAGE_RETRIES):
        try:
            response = session.get(
                base_url,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            return response

        except requests.exceptions.Timeout:
            if attempt < MAX_PAGE_RETRIES - 1:
                wait = 15 * (attempt + 1)  # 15s, 30s, 45s, 60s
                logger.warning(
                    f"[fetcher] Page {page} timed out. "
                    f"Retrying in {wait}s (attempt {attempt + 1}/{MAX_PAGE_RETRIES})..."
                )
                time.sleep(wait)
            else:
                logger.error(f"[fetcher] Page {page} failed after {MAX_PAGE_RETRIES} attempts")
                raise

        except requests.exceptions.ConnectionError as e:
            if attempt < MAX_PAGE_RETRIES - 1:
                wait = 15 * (attempt + 1)
                logger.warning(
                    f"[fetcher] Page {page} connection error: {e}. "
                    f"Retrying in {wait}s (attempt {attempt + 1}/{MAX_PAGE_RETRIES})..."
                )
                time.sleep(wait)
            else:
                logger.error(f"[fetcher] Page {page} connection failed after {MAX_PAGE_RETRIES} attempts")
                raise


def fetch_nys_paginated_to_gcs(blob_name: str) -> str:
    """
    Fetches NYS corporations with two filters applied at the Socrata level:

    1. $where  — county IN (5 NYC boroughs) — cuts 4M rows to ~200-400K
    2. $select — only the 4 columns we use — cuts storage significantly

    Paginates through 50K-row pages until empty, combines into one CSV,
    uploads to GCS.
    """
    county_list = ", ".join(f"'{c}'" for c in NYC_COUNTIES)
    where_clause = f"county IN ({county_list})"

    base_url = f"https://{NYS_DOMAIN}/resource/{NYS_DATASET}.csv"
    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN
        logger.info("[fetcher] Using Socrata app token")
    else:
        logger.warning("[fetcher] No SOCRATA_APP_TOKEN — requests may be rate limited")

    session = make_session()

    all_rows = []
    csv_headers = None
    offset = 0
    page = 0

    logger.info(f"[fetcher] Starting paginated NYS fetch")
    logger.info(f"[fetcher] Filter: {where_clause}")
    logger.info(f"[fetcher] Columns: {NYS_COLUMNS}")

    while True:
        params = {
            "$select": NYS_COLUMNS,
            "$where": where_clause,
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": ":id",
        }

        logger.info(f"[fetcher] NYS page {page}: rows {offset} -> {offset + PAGE_SIZE}...")

        response = fetch_page_with_retry(session, base_url, params, headers, page)

        text = response.text
        reader = csv.DictReader(io.StringIO(text))

        if csv_headers is None:
            csv_headers = reader.fieldnames
            logger.info(f"[fetcher] NYS columns returned: {csv_headers}")

        page_rows = list(reader)

        if not page_rows:
            logger.info(f"[fetcher] NYS fetch complete — {len(all_rows)} total rows, {page} pages")
            break

        all_rows.extend(page_rows)
        logger.info(
            f"[fetcher] NYS page {page}: {len(page_rows)} rows "
            f"(running total: {len(all_rows)})"
        )

        if len(page_rows) < PAGE_SIZE:
            logger.info(f"[fetcher] NYS fetch complete — {len(all_rows)} total rows, {page + 1} pages")
            break

        offset += PAGE_SIZE
        page += 1

    if not all_rows:
        raise ValueError(
            "NYS paginated fetch returned 0 rows — check county filter and app token"
        )

    logger.info(f"[fetcher] Writing {len(all_rows)} NYS rows to GCS...")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=csv_headers)
    writer.writeheader()
    writer.writerows(all_rows)

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(output.getvalue(), content_type="text/csv")

    gcs_url = f"gs://{GCS_BUCKET_NAME}/{blob_name}"
    logger.info(f"[fetcher] Uploaded filtered NYS data -> {gcs_url}")
    return gcs_url


# ============================================================
# Main Entry Point
# ============================================================

def run():
    logger.info("=== Fetcher starting ===")

    # NYC DCA — bulk download, already NYC-only
    try:
        nyc_url = stream_bulk_to_gcs(
            NYC_DOMAIN,
            NYC_DATASET,
            "raw/nyc-dca-businesses.csv",
        )
        logger.info(f"[fetcher] NYC complete: {nyc_url}")
    except Exception as e:
        logger.error(f"[fetcher] NYC fetch failed: {e}")
        raise

    # NYS corporations — paginated, filtered to NYC counties, 4 columns only
    try:
        nys_url = fetch_nys_paginated_to_gcs("raw/nys-corporations.csv")
        logger.info(f"[fetcher] NYS complete: {nys_url}")
    except Exception as e:
        logger.error(f"[fetcher] NYS fetch failed: {e}")
        raise

    logger.info("=== Fetcher complete ===")
    return nyc_url, nys_url


if __name__ == "__main__":
    run()