"""
fetcher.py

Fetches two datasets from Socrata open data APIs:
  1. NYC DCA Legally Operating Businesses (data.cityofnewyork.us)
  2. NY State Corporations & Entities (data.ny.gov)

Paginates through all records using httpx, then uploads
each dataset as a CSV to a GCS bucket.
"""

import asyncio
import csv
import io
import logging
import os

import httpx
from dotenv import load_dotenv
from google.cloud import storage

# Section 1: Config
# load_dotenv() reads your .env file and makes all the variables inside it available via os.getenv()
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Socrata dataset identifiers
NYC_DOMAIN = "data.cityofnewyork.us"
NYC_DATASET = "w7w3-xahh" # NYC DCA Legally Operating Business

NYS_DOMAIN = "data.ny.gov"
NYS_DATASET = "p3qf-k9ut" # NY State Corporations & Entites

# Socrata hard maximum is 50k records per request, so loop with this page size until we have everything
PAGE_SIZE = 50_000

# Read bucket name from .env
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# Section 2: The Paginator
# Ask for page 1 (offset 0), then page 2 (offset 50000), then page 3 (offset 100000), and so until a page comes back empty
# httpx.AsyncClient is like the 'requests' library but async.

async def fetch_all_records(domain: str, dataset_id: str) -> list[dict]:
    """
    Paginate through an entire Socrata dataset.

    Args:
        domain:     e.g. "data.cityofnewyork.us"
        dataset_id: e.g. "w7w3-xahh"

    Returns:
        All records as a list of dicts
    """
    url = f"https://{domain}/resource/{dataset_id}.json"
    all_records = []
    offset = 0

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            params = {
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$order": ":id" # consistent ordering across pages
            }

            logger.info(f"Fetching {domain}/{dataset_id} | offset={offset} | total so far={len(all_records)}")

            response = await client.get(url, params=params)

            # If the API returns an error, raise_forstauts() will crash loudly so we know
            response.raise_for_status()

            page = response.json()

            # Empty page means we've fected all records
            if not page:
                logger.info(f"Finished {dataset_id}. Total records: {len(all_records)}")
                break
            
            all_records.extend(page)
            offset += PAGE_SIZE
    return all_records

# Records to CSV
# API returns a list of dicts like [{"business_name": "Joe's Pizza", "license": "123"}, ...]
# Need to convert this to a CSV
# Use io.StringIO -- lives in memory

def records_to_csv_bytes(records: list[dict]) -> bytes:
    '''
    Convert a list of dicts into CSV bytes in memory.
    The keys of the first record become the column headers.
    '''
    if not records:
        return b""

    output = io.StringIO()
    headers = list(records[0].keys())
    writer = csv.DictWriter(output, fieldnames=headers, extrasaction="ignore")

    writer.writeheader()
    writer.writerows(records)

    # Encode string to bytes so GCS can store it
    return output.getvalue().encode("utf-8")

## Upload to GCS
# Use google-cloud-storage python library
# storage.clinet() automtically reads creds from GOOGLE_APPLICATION_CREDENTIALS env variable which points to gcp-credentials.json
# A "blob" in GCS is just a file. You give it a name (the path inside the bucket) and upload bytes to it.
def upload_to_gcs(data: bytes, blob_name: str) -> str:
    '''
    Upload bytes to GCS and return the gs:// URL.

    Args:
        data: CSV file as bytes
        blob_name: path inside the bucket e.g "raw/nyc-dca-businesses.csv"

    Returns:
        GCS Url like gs://baselayer-kyb-raw-data/raw/nyc-dca-businesses.csv
    '''

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type="text/csv")

    gcs_url = f"gs://{GCS_BUCKET_NAME}/{blob_name}"
    logger.info(f"Uploaded -> {gcs_url}")
    return gcs_url

# Orchestrator
# Tie all 3 functions together. Fetch both datasets, convert them to CSV, upload them to GCS
# The URLs returned here (gs://...) are what models.py will use in the next step to read the files with Apache Beam
async def run() -> tuple[str, str]:
    logger.info("== Fetcher starting ===")

    # Dataset 1: NYC DCA Business
    logger.info("--- Fetching NYC DCA Legally Operating Businesses ---")
    nyc_records = await fetch_all_records(NYC_DOMAIN, NYC_DATASET)
    nyc_csv = records_to_csv_bytes(nyc_records)
    nyc_url = upload_to_gcs(nyc_csv, "raw/nyc-dca-businesses.csv")
    logger.info(f"NYC done | rows={len(nyc_records)} | url={nyc_url}")

    # Dataset 2: NY State Corporations
    logger.info("--- Fetching NY State Corporations & Entities ---")
    nys_records = await fetch_all_records(NYS_DOMAIN, NYS_DATASET)
    nys_csv = records_to_csv_bytes(nys_records)
    nys_url = upload_to_gcs(nys_csv, "raw/nys-corporations.csv")
    logger.info(f"NYS done | rows={len(nys_records)} | url={nys_url}")

    logger.info("=== Fetcher complete ===")
    logger.info(f"NYC -> {nyc_url}")
    logger.info(f"NYS -> {nys_url}")

    return nyc_url, nys_url

# Entry Point
# asyncio.run() is required to execute async Python code.
# When Python sees 'async def' it needs asyncio to actually run it
# When Docker runs 'python fetcher.py' this is the line that kicks everything off.
if __name__ == "__main__":
    asyncio.run(run())