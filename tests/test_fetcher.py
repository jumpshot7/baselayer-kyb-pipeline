"""
test_fetcher.py

Tests for fetcher.py. We mock external HTTP requests (httpx)
and Google Cloud Storage (google.cloud.storage) to keep tests
fast and isolated.
"""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import sys
from pathlib import Path

# Add backend to path so we can import fetcher
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from fetcher import fetch_all_records, records_to_csv_bytes, upload_to_gcs, run

# -------------------------------------------------------
# SECTION 1: Test CSV Conversion
# -------------------------------------------------------
def test_records_to_csv_bytes_empty():
    """Test that passing an empty list returns empty bytes."""
    result = records_to_csv_bytes([])
    assert result == b""

def test_records_to_csv_bytes_success():
    """Test that valid records are converted to CSV bytes with headers."""
    records = [
        {"id": "1", "name": "Joe's Pizza"},
        {"id": "2", "name": "Mario's Deli"}
    ]
    result = records_to_csv_bytes(records)
    
    # Decode bytes to string so we can easily check the content
    csv_string = result.decode("utf-8")
    
    # It should have a header row and data rows
    assert "id,name" in csv_string
    assert "1,Joe's Pizza" in csv_string
    assert "2,Mario's Deli" in csv_string


# -------------------------------------------------------
# SECTION 2: Test Fetching Logic (httpx mocking)
# -------------------------------------------------------
@pytest.mark.asyncio
@patch("fetcher.httpx.AsyncClient")
async def test_fetch_all_records(mock_async_client_class):
    """
    Test that fetch_all_records paginates correctly.
    We mock the async httpx client to return one page of data,
    then an empty page to break the loop.
    """
    # Create a mock for the client instance
    mock_client = AsyncMock()
    
    # When using an async context manager (async with ...), we need to set up the return value
    mock_async_client_class.return_value.__aenter__.return_value = mock_client
    
    # Setup the mock responses
    mock_response_page_1 = MagicMock()
    mock_response_page_1.json.return_value = [{"id": 1}, {"id": 2}]
    
    mock_response_page_2 = MagicMock()
    mock_response_page_2.json.return_value = [] # Empty page stops the loop
    
    # client.get will return page_1 on first call, then page_2 on second call
    mock_client.get.side_effect = [mock_response_page_1, mock_response_page_2]
    
    domain = "test.domain"
    dataset_id = "test-dataset"
    
    records = await fetch_all_records(domain, dataset_id)
    
    # We should have received the two records from page 1
    assert len(records) == 2
    assert records[0]["id"] == 1
    
    # It should have called get() twice
    assert mock_client.get.call_count == 2
    
    # Both calls should check raise_for_status
    assert mock_response_page_1.raise_for_status.called
    assert mock_response_page_2.raise_for_status.called


# -------------------------------------------------------
# SECTION 3: Test Upload to GCS (google-cloud-storage mocking)
# -------------------------------------------------------
@patch("fetcher.storage.Client")
@patch("fetcher.GCS_BUCKET_NAME", "test-bucket")
def test_upload_to_gcs(mock_storage_client_class):
    """
    Test uploading bytes to GCS without making actual GCP calls.
    """
    # Set up the mock objects mapping to bucket and blob
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    
    mock_storage_client_class.return_value = mock_client
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    
    # Run the function
    test_data = b"some,fake,csv,data"
    result_url = upload_to_gcs(test_data, "raw/test.csv")
    
    # Verify the bucket was requested using our fake bucket name
    mock_client.bucket.assert_called_once_with("test-bucket")
    
    # Verify the blob was created with the right filename
    mock_bucket.blob.assert_called_once_with("raw/test.csv")
    
    # Verify it uploaded the exact bytes we provided as text/csv
    mock_blob.upload_from_string.assert_called_once_with(test_data, content_type="text/csv")
    
    # Verify the returned URL is formatted correctly
    assert result_url == "gs://test-bucket/raw/test.csv"


# -------------------------------------------------------
# SECTION 4: Test Orchestrator (run)
# -------------------------------------------------------
@pytest.mark.asyncio
@patch("fetcher.fetch_all_records")
@patch("fetcher.upload_to_gcs")
async def test_run_orchestrator(mock_upload, mock_fetch):
    """
    Test the main run() function orchestrates fetching and uploading
    for both the NYC and NYS datasets.
    """
    # Mock return values for fetch_all_records
    mock_fetch.side_effect = [
        [{"id": "nyc1"}], # Return for NYC data
        [{"id": "nys1"}]  # Return for NYS data
    ]
    
    # Mock return values for upload_to_gcs
    mock_upload.side_effect = [
        "gs://test/nyc.csv",
        "gs://test/nys.csv"
    ]
    
    # Call the orchestrator
    nyc_url, nys_url = await run()
    
    # Check that it fetched both datasets
    assert mock_fetch.call_count == 2
    
    # Check that it uploaded both datasets
    assert mock_upload.call_count == 2
    
    # Check that it returns the URLs generated by upload_to_gcs
    assert nyc_url == "gs://test/nyc.csv"
    assert nys_url == "gs://test/nys.csv"