"""
fetcher.py

Downloads the full datasets as CSVs directly from Socrata's bulk export endpoints
and streams them directly to Google Cloud Storage to prevent Out-Of-Memory errors.
"""

import logging
import os
import requests
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

def stream_socrata_to_gcs(domain: str, dataset_id: str, blob_name: str) -> str:
    """
    Downloads a Socrata dataset using the Bulk CSV Export endpoint and streams
    it directly to GCS so we don't hold 30 million rows in memory.
    """
    # Socrata's hidden bulk CSV download URL
    url = f"https://{domain}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"
    
    logger.info(f"Starting bulk download for {dataset_id}...")
    
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)

    # Socrata send bulk files as GZIp. Set headers to ask for identity
    # or let requests decompress it. Setting stream=True and reading it
    # via response.raw bypasses requests automatic decompression.
    headers = {"Accept-Encoding": "identity"}
    with requests.get(url, stream=True, headers=headers, timeout=120) as response:
        response.raise_for_status()
        
        # Upload the stream directly to Google Cloud Storage
        # upload_from_file reads the network stream piece by piece
        blob.upload_from_file(response.raw, content_type="text/csv")
        
    gcs_url = f"gs://{GCS_BUCKET_NAME}/{blob_name}"
    logger.info(f"Successfully streamed {dataset_id} directly to -> {gcs_url}")
    return gcs_url

def run():
    logger.info("=== Fetcher starting ===")

    # Since this is no longer async, we just call the functions directly
    nyc_url = stream_socrata_to_gcs(NYC_DOMAIN, NYC_DATASET, "raw/nyc-dca-businesses.csv")
    nys_url = stream_socrata_to_gcs(NYS_DOMAIN, NYS_DATASET, "raw/nys-corporations.csv")

    logger.info("=== Fetcher complete ===")
    return nyc_url, nys_url

if __name__ == "__main__":
    run()