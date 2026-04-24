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
import tempfile
import shutil


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
    Downloads a Socrata dataset using the Bulk CSV Export endpoint to a temporary
    local file, then uploads that file to GCS so we don't hold 30 million rows in memory.
    """
    # Socrata's hidden bulk CSV download URL
    url = f"https://{domain}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"
    logger.info(f"[fetcher] START fetch {dataset_id} from {domain}")
    
    logger.info(f"Starting bulk download for {dataset_id}...")
    
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)

    # Socrata send bulk files as GZip. Set headers to ask for identity
    # or let requests decompress it. Setting stream=True and reading it
    # via response.raw bypasses requests automatic decompression.
    headers = {"Accept-Encoding": "identity"}
    with requests.get(url, stream=True, headers=headers, timeout=120) as response:
        response.raise_for_status()
        
        # Write the downloaded stream to a temporary file locally.
        # Temp files get deleted automatically when the 'with' block closes.
        with tempfile.NamedTemporaryFile() as tmp_file:
            logger.info(f"[fetcher] DOWNLOAD BEGIN {dataset_id}")
            bytes_downloaded = 0
            # Download safely chunk by chunk
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    tmp_file.write(chunk)
                    bytes_downloaded += len(chunk)
                    if bytes_downloaded % (10 * 1024 * 1024) < len(chunk):
                        logger.info(
                            f"[fetcher] {dataset_id} downloaded {bytes_downloaded // (1024*1024)} MB"
                        )
            
            # Make sure everything is flushed to the disk
            tmp_file.flush()

            logger.info(f"[fetcher] DOWNLOAD COMPLETE {dataset_id} ({bytes_downloaded} bytes)")
            logger.info("Download complete. Uploading the file to Google Cloud Storage")
            # Upload the whole, completed file securely
            blob.upload_from_filename(tmp_file.name, content_type="text/csv")
        
    gcs_url = f"gs://{GCS_BUCKET_NAME}/{blob_name}"
    logger.info(f"Successfully streamed {dataset_id} directly to -> {gcs_url}")
    
    return gcs_url

def run():
    logger.info("=== Fetcher starting ===")

    # Since this is no longer async, we just call the functions directly
    try:
        nyc_url = stream_socrata_to_gcs(NYC_DOMAIN, NYC_DATASET, "raw/nyc-dca-businesses.csv")
    except Exception as e:
        logger.error(f"[fetcher] NYC fetch failed: {e}")
        raise
    try:
        nys_url = stream_socrata_to_gcs(NYS_DOMAIN, NYS_DATASET, "raw/nys-corporations.csv")
    except Exception as e:
        logger.error(f"[fetcher] NYS fetch failed: {e}")
        raise
    
    logger.info("=== Fetcher complete ===")
    return nyc_url, nys_url

if __name__ == "__main__":
    run()