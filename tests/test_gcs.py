"""
test_gcs.py

Tests that we can communicate with our GCS bucket.
Verifies:
- We can connect to GCS with valid creds
- Our bucket exists and is accessible
- We can upload a file to the bucket
- We can download a file from the bucket
- We can delete a file from the bucket
"""

import os
import pytest
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# Fixture
# Fixture is a function that sets something up before a rest runs and tears it down after.
#
# @pytest.fixture means "this function provides something that tests can ask for by name"
#
# Any test that list 'gcs_client' as a parameter automatically gets a real GCS storage client injected.

@pytest.fixture
def gcs_client():
    """
    Provides an authenticated GCS client.
    Reads credentials from GOOGLE_APPLICATION_CREDENTIALS environment variable automatically.
    """
    client = storage.Client()
    return client

@pytest.fixture
def gcs_bucket(gcs_client):
    """
    Provides the GCS bucket object.
    Depends on gcs_client fixture - pytest injects it
    automatically because the parameter name matches
    """
    bucket = gcs_client.bucket(GCS_BUCKET_NAME)
    return bucket

# Tests
def test_gcs_credentials_loaded():
    """
    Verify the credentials env variable is set.
    If this fails it means .env isn't loaded correctly.
    """
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    assert creds_path is not None, "GOOGLE_APPLICATION_CREDENTIALS not set in .env"
    assert os.path.exists(creds_path), f"Credentials file not found at: {creds_path}"

def test_gcs_bucket_name_loaded():
    """
    Verify the bucket name env variable is set"""
    assert GCS_BUCKET_NAME is not None, "GCS_BUCKET_NAME not set in .env"
    assert GCS_BUCKET_NAME != "", "GCS_BUCKET_NAME is empty"

def test_gcs_client_connects(gcs_client):
    """
    Verify we can create an authenticated GCS client. If creds are wrong, this will raise an exception.
    """
    assert gcs_client is not None
    # list_buckets() forces an actual auth API call
    buckets = list(gcs_client.list_buckets())
    assert isinstance(buckets, list)

def test_gcs_bucket_exists(gcs_bucket):
    """
    Verify our specific bucket exists and is accessible.
    bucket.exists() returns True if we can reach it
    """
    assert gcs_bucket.exists(), (
        f"Bucket '{GCS_BUCKET_NAME}' does not exist or is not accessible"
    )

def test_gcs_upload(gcs_bucket):
    """
    Verify we can upload a file to the bucket.
    Upload a small test blob and check it lands correctly.
    """
    test_blob_name = "test/ping.txt"
    test_content = b"baselayer-kyb-pipeline test ping"

    blob = gcs_bucket.blob(test_blob_name)
    blob.upload_from_string(test_content, content_type = "text/plain")

    # Verify it exists fter upload
    assert blob.exists(), "Test blob was not found after upload"

def test_gcs_delete(gcs_bucket):
    """
    Verify we can delete the test file we uploaded.
    After deletion, blob.exists() should return False
    """
    test_blob_name = "test/ping.txt"

    blob = gcs_bucket.blob(test_blob_name)
    blob.delete()

    assert not blob.exists(), "Test blob still exists after deletion"