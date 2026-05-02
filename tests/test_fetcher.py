"""
test_fetcher.py

Tests for fetcher.py. We mock external HTTP requests (requests)
and Google Cloud Storage (google.cloud.storage) to keep tests
fast and isolated.

The actual fetcher.py has these main functions:
- make_session()            -> returns a requests.Session with retry logic
- stream_bulk_to_gcs()      -> downloads full NYC CSV via bulk export, uploads to GCS
- fetch_page_with_retry()   -> fetches a single paginated page with retry logic
- fetch_nys_paginated_to_gcs() -> paginates NYS API with filters, uploads to GCS
- run()                     -> orchestrates both fetches
"""

import pytest
from unittest.mock import patch, MagicMock, call
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from fetcher import (
    make_session,
    fetch_page_with_retry,
    run,
    NYC_COUNTIES,
    NYS_COLUMNS,
    PAGE_SIZE,
)


# -------------------------------------------------------
# SECTION 1: make_session()
# -------------------------------------------------------

def test_make_session_returns_session():
    """
    make_session() should return a requests.Session object
    with retry adapters mounted on http:// and https://.
    """
    session = make_session()
    import requests
    assert isinstance(session, requests.Session)
    # Verify adapters are mounted
    assert "https://" in session.adapters
    assert "http://" in session.adapters


# -------------------------------------------------------
# SECTION 2: fetch_page_with_retry()
# -------------------------------------------------------

def test_fetch_page_with_retry_success():
    """
    fetch_page_with_retry() should return the response
    on a successful first attempt.
    """
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_session.get.return_value = mock_response

    result = fetch_page_with_retry(
        session=mock_session,
        base_url="https://test.domain/resource/test.csv",
        params={"$limit": 50000, "$offset": 0},
        headers={},
        page=0,
    )

    assert result == mock_response
    mock_session.get.assert_called_once()
    mock_response.raise_for_status.assert_called_once()


def test_fetch_page_with_retry_retries_on_timeout():
    """
    fetch_page_with_retry() should retry on Timeout
    and eventually return the response if a later attempt succeeds.
    """
    import requests

    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    # First call raises Timeout, second call succeeds
    mock_session.get.side_effect = [
        requests.exceptions.Timeout(),
        mock_response,
    ]

    with patch("fetcher.time.sleep"):  # Skip actual sleep in tests
        result = fetch_page_with_retry(
            session=mock_session,
            base_url="https://test.domain/resource/test.csv",
            params={"$limit": 50000, "$offset": 0},
            headers={},
            page=0,
        )

    assert result == mock_response
    assert mock_session.get.call_count == 2


def test_fetch_page_with_retry_raises_after_max_retries():
    """
    fetch_page_with_retry() should raise after MAX_PAGE_RETRIES
    consecutive timeouts.
    """
    import requests

    mock_session = MagicMock()
    mock_session.get.side_effect = requests.exceptions.Timeout()

    with patch("fetcher.time.sleep"):
        with pytest.raises(requests.exceptions.Timeout):
            fetch_page_with_retry(
                session=mock_session,
                base_url="https://test.domain/resource/test.csv",
                params={"$limit": 50000, "$offset": 0},
                headers={},
                page=0,
            )


# -------------------------------------------------------
# SECTION 3: stream_bulk_to_gcs()
# -------------------------------------------------------

@patch("fetcher.storage.Client")
@patch("fetcher.GCS_BUCKET_NAME", "test-bucket")
@patch("fetcher.make_session")
def test_stream_bulk_to_gcs_uploads_to_gcs(mock_make_session, mock_storage_client_class):
    """
    stream_bulk_to_gcs() should download the CSV and upload it to GCS.
    """
    # Mock the requests session and response
    mock_session = MagicMock()
    mock_make_session.return_value = mock_session

    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"col1,col2\n", b"val1,val2\n"]
    mock_response.__enter__ = lambda s: mock_response
    mock_response.__exit__ = MagicMock(return_value=False)
    mock_session.get.return_value = mock_response

    # Mock GCS
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client_class.return_value = mock_client
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    from fetcher import stream_bulk_to_gcs
    result = stream_bulk_to_gcs("data.cityofnewyork.us", "w7w3-xahh", "raw/nyc-dca-businesses.csv")

    # Verify GCS upload was called
    mock_blob.upload_from_filename.assert_called_once()
    assert result == "gs://test-bucket/raw/nyc-dca-businesses.csv"


# -------------------------------------------------------
# SECTION 4: fetch_nys_paginated_to_gcs()
# -------------------------------------------------------

@patch("fetcher.storage.Client")
@patch("fetcher.GCS_BUCKET_NAME", "test-bucket")
@patch("fetcher.make_session")
def test_fetch_nys_paginated_to_gcs_single_page(mock_make_session, mock_storage_client_class):
    """
    fetch_nys_paginated_to_gcs() should paginate and stop when
    it gets fewer rows than PAGE_SIZE.
    """
    mock_session = MagicMock()
    mock_make_session.return_value = mock_session

    # Simulate one page of results smaller than PAGE_SIZE
    csv_content = "dos_id,current_entity_name,initial_dos_filing_date,dos_process_zip\n"
    csv_content += "123,TEST CORP,2020-01-01,10001\n"
    csv_content += "456,ANOTHER CORP,2019-05-15,11201\n"

    mock_response = MagicMock()
    mock_response.text = csv_content

    with patch("fetcher.fetch_page_with_retry", return_value=mock_response):
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        from fetcher import fetch_nys_paginated_to_gcs
        result = fetch_nys_paginated_to_gcs("raw/nys-corporations.csv")

    mock_blob.upload_from_string.assert_called_once()
    assert result == "gs://test-bucket/raw/nys-corporations.csv"


@patch("fetcher.storage.Client")
@patch("fetcher.GCS_BUCKET_NAME", "test-bucket")
@patch("fetcher.make_session")
def test_fetch_nys_paginated_stops_on_empty_page(mock_make_session, mock_storage_client_class):
    """
    fetch_nys_paginated_to_gcs() should stop paginating
    when it receives an empty page.
    """
    mock_session = MagicMock()
    mock_make_session.return_value = mock_session

    # First page has data, second page is empty (just headers)
    page1_csv = "dos_id,current_entity_name,initial_dos_filing_date,dos_process_zip\n"
    for i in range(PAGE_SIZE):
        page1_csv += f"{i},CORP {i},2020-01-01,10001\n"

    page2_csv = "dos_id,current_entity_name,initial_dos_filing_date,dos_process_zip\n"

    mock_resp1 = MagicMock()
    mock_resp1.text = page1_csv
    mock_resp2 = MagicMock()
    mock_resp2.text = page2_csv

    with patch("fetcher.fetch_page_with_retry", side_effect=[mock_resp1, mock_resp2]):
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        from fetcher import fetch_nys_paginated_to_gcs
        result = fetch_nys_paginated_to_gcs("raw/nys-corporations.csv")

    assert result == "gs://test-bucket/raw/nys-corporations.csv"


# -------------------------------------------------------
# SECTION 5: Constants
# -------------------------------------------------------

def test_nyc_counties_contains_all_boroughs():
    """
    NYC_COUNTIES should contain all 5 NYC borough county names
    exactly as they appear in the Socrata dataset.
    """
    assert "New York" in NYC_COUNTIES   # Manhattan
    assert "Kings" in NYC_COUNTIES      # Brooklyn
    assert "Queens" in NYC_COUNTIES     # Queens
    assert "Bronx" in NYC_COUNTIES      # Bronx
    assert "Richmond" in NYC_COUNTIES   # Staten Island
    assert len(NYC_COUNTIES) == 5


def test_nys_columns_contains_required_fields():
    """
    NYS_COLUMNS should include all 4 columns we actually use
    in the pipeline.
    """
    assert "dos_id" in NYS_COLUMNS
    assert "current_entity_name" in NYS_COLUMNS
    assert "initial_dos_filing_date" in NYS_COLUMNS
    assert "dos_process_zip" in NYS_COLUMNS


# -------------------------------------------------------
# SECTION 6: run() orchestrator
# -------------------------------------------------------

@patch("fetcher.fetch_nys_paginated_to_gcs")
@patch("fetcher.stream_bulk_to_gcs")
def test_run_calls_both_fetchers(mock_bulk, mock_paginated):
    """
    run() should call stream_bulk_to_gcs for NYC data
    and fetch_nys_paginated_to_gcs for NYS data.
    """
    mock_bulk.return_value = "gs://test/nyc.csv"
    mock_paginated.return_value = "gs://test/nys.csv"

    nyc_url, nys_url = run()

    assert mock_bulk.call_count == 1
    assert mock_paginated.call_count == 1
    assert nyc_url == "gs://test/nyc.csv"
    assert nys_url == "gs://test/nys.csv"


@patch("fetcher.fetch_nys_paginated_to_gcs")
@patch("fetcher.stream_bulk_to_gcs")
def test_run_raises_if_nyc_fetch_fails(mock_bulk, mock_paginated):
    """
    run() should raise and not attempt the NYS fetch
    if the NYC fetch fails.
    """
    mock_bulk.side_effect = Exception("NYC fetch failed")

    with pytest.raises(Exception, match="NYC fetch failed"):
        run()

    mock_paginated.assert_not_called()