"""
test_api.py

Tests for api.py using FastAPI's TestClient.
TestClient simulates real HTTP requests without
needing a running server.

We mock the database calls so tests are fast
and don't need a real Postgres connection.
"""

import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from api import app

# -------------------------------------------------------
# SECTION 1: Test Client Setup
# -------------------------------------------------------
# TestClient wraps your FastAPI app and lets you make
# HTTP requests against it in memory.
# It's like Postman but inside your test file.
# @pytest.fixture means every test that lists `client`
# as a parameter gets this automatically injected.
@pytest.fixture
def client():
    return TestClient(app)


# -------------------------------------------------------
# SECTION 2: Health Check Tests
# -------------------------------------------------------

def test_health_returns_200(client):
    """
    GET /health should always return 200 OK.
    We mock query_one to return a fake count so
    the test doesn't need a real DB connection.
    """
    with patch("api.query_one", return_value={"count": 42}):
        response = client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["anomaly_count"] == 42


def test_health_returns_500_on_db_error(client):
    """
    GET /health should return 500 if the DB is down.
    We simulate a DB failure by making query_one raise
    an exception.
    """
    with patch("api.query_one", side_effect=Exception("DB connection failed")):
        response = client.get("/health")

    assert response.status_code == 500


# -------------------------------------------------------
# SECTION 3: Anomaly Endpoint Tests
# -------------------------------------------------------

def test_get_anomalies_returns_200(client):
    """
    GET /anomalies should return 200 with a results list.
    We mock query() to return two fake anomaly records.
    """
    fake_anomalies = [
        {
            "id": 1,
            "match_score": 92.5,
            "business_name": "JOES PIZZA LLC",
            "current_entity_name": "JOES PIZZA LIMITED LIABILITY",
            "has_anomaly": True,
            "flag_license_active_entity_dissolved": True,
            "flag_license_predates_formation": False,
            "flag_entity_dormant": False,
            "flag_address_mismatch": False,
            "license_number": "123456",
            "license_status": "Active",
            "license_type": "Retail",
            "expiration_date": "2025-01-01",
            "borough": "Brooklyn",
            "nyc_zip": "11201",
            "dos_id": "987654",
            "entity_type": "LLC",
            "dos_process_name": "Active",
            "date_of_formation": "2015-01-01",
            "date_of_dissolution": "2021-06-01",
            "nys_zip": "11201",
            "created_at": "2024-01-01T00:00:00",
        },
        {
            "id": 2,
            "match_score": 88.0,
            "business_name": "MARIOS DELI",
            "current_entity_name": "MARIO DELI INC",
            "has_anomaly": True,
            "flag_license_active_entity_dissolved": False,
            "flag_license_predates_formation": True,
            "flag_entity_dormant": False,
            "flag_address_mismatch": False,
            "license_number": "654321",
            "license_status": "Active",
            "license_type": "Food",
            "expiration_date": "2025-06-01",
            "borough": "Queens",
            "nyc_zip": "11370",
            "dos_id": "111222",
            "entity_type": "Corporation",
            "dos_process_name": "Active",
            "date_of_formation": "2019-01-01",
            "date_of_dissolution": None,
            "nys_zip": "11370",
            "created_at": "2024-01-01T00:00:00",
        },
    ]

    with patch("api.query", return_value=fake_anomalies):
        response = client.get("/anomalies")

    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    assert len(data["results"]) == 2
    assert data["count"] == 2


def test_get_anomalies_empty_results(client):
    """
    GET /anomalies should return 200 with empty list
    when no anomalies exist yet.
    """
    with patch("api.query", return_value=[]):
        response = client.get("/anomalies")

    assert response.status_code == 200
    data = response.json()
    assert data["results"] == []
    assert data["count"] == 0


def test_get_anomaly_by_id_not_found(client):
    """
    GET /anomalies/99999 should return 404 when
    the anomaly ID doesn't exist in the database.
    """
    with patch("api.query_one", return_value=None):
        response = client.get("/anomalies/99999")

    assert response.status_code == 404


def test_get_anomaly_by_id_found(client):
    """
    GET /anomalies/1 should return 200 with the
    full anomaly record when it exists.
    """
    fake_anomaly = {
        "id": 1,
        "match_score": 92.5,
        "has_anomaly": True,
        "nyc_business": {"business_name": "JOES PIZZA LLC"},
        "nys_entity": {"current_entity_name": "JOES PIZZA LIMITED LIABILITY"},
    }

    with patch("api.query_one", return_value=fake_anomaly):
        response = client.get("/anomalies/1")

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == 1
    assert data["match_score"] == 92.5


def test_get_anomaly_summary(client):
    """
    GET /anomalies/summary should return counts
    for each anomaly flag type.
    """
    fake_summary = {
        "total_anomalies": 150,
        "flag_license_active_entity_dissolved": 45,
        "flag_license_predates_formation": 30,
        "flag_entity_dormant": 60,
        "flag_address_mismatch": 15,
    }

    with patch("api.query_one", return_value=fake_summary):
        response = client.get("/anomalies/summary")

    assert response.status_code == 200
    data = response.json()
    assert data["total_anomalies"] == 150
    assert data["flag_license_active_entity_dissolved"] == 45
    assert data["flag_entity_dormant"] == 60


def test_get_dissolved_anomalies(client):
    """
    GET /anomalies/dissolved should return only
    records where the entity is dissolved.
    """
    fake_results = [
        {
            "business_name": "JOES PIZZA LLC",
            "license_number": "123456",
            "license_status": "Active",
            "expiration_date": "2025-01-01",
            "borough": "Brooklyn",
            "current_entity_name": "JOES PIZZA LIMITED LIABILITY",
            "date_of_dissolution": "2021-06-01",
            "entity_type": "LLC",
            "match_score": 92.5,
        }
    ]

    with patch("api.query", return_value=fake_results):
        response = client.get("/anomalies/dissolved")

    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 1
    assert data["results"][0]["business_name"] == "JOES PIZZA LLC"


def test_get_predates_anomalies(client):
    """
    GET /anomalies/predates should return records where
    the license was issued before entity formation.
    """
    fake_results = [
        {
            "business_name": "MARIOS DELI",
            "license_number": "654321",
            "initial_issuance_date": "2010-01-01",
            "current_entity_name": "MARIO DELI INC",
            "date_of_formation": "2015-01-01",
            "match_score": 88.0,
        }
    ]

    with patch("api.query", return_value=fake_results):
        response = client.get("/anomalies/predates")

    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 1
    assert data["results"][0]["business_name"] == "MARIOS DELI"


# -------------------------------------------------------
# SECTION 4: Borough Endpoint Tests
# -------------------------------------------------------

def test_get_anomalies_by_borough(client):
    """
    GET /anomalies/by-borough/Brooklyn should return
    only anomalies for businesses in Brooklyn.
    """
    fake_results = [
        {
            "business_name": "JOES PIZZA LLC",
            "license_number": "123456",
            "license_status": "Active",
            "borough": "Brooklyn",
            "current_entity_name": "JOES PIZZA LIMITED LIABILITY",
            "date_of_dissolution": "2021-06-01",
            "match_score": 92.5,
            "has_anomaly": True,
            "flag_license_active_entity_dissolved": True,
            "flag_license_predates_formation": False,
            "flag_entity_dormant": False,
            "flag_address_mismatch": False,
        }
    ]

    with patch("api.query", return_value=fake_results):
        response = client.get("/anomalies/by-borough/Brooklyn")

    assert response.status_code == 200
    data = response.json()
    assert data["borough"] == "Brooklyn"
    assert len(data["results"]) == 1
    assert data["results"][0]["borough"] == "Brooklyn"


# -------------------------------------------------------
# SECTION 5: Business Search Endpoint Tests
# -------------------------------------------------------

def test_search_businesses_returns_results(client):
    """
    GET /businesses/search?name=pizza should return
    matching businesses.
    """
    fake_results = [
        {
            "id": 1,
            "license_number": "123456",
            "business_name": "JOES PIZZA LLC",
            "dba_trade_name": None,
            "license_status": "Active",
            "license_type": "Retail",
            "business_category": "Food",
            "expiration_date": "2025-01-01",
            "borough": "Brooklyn",
            "zip_code": "11201",
        }
    ]

    with patch("api.query", return_value=fake_results):
        response = client.get("/businesses/search?name=pizza")

    assert response.status_code == 200
    data = response.json()
    assert data["query"] == "pizza"
    assert len(data["results"]) == 1
    assert data["results"][0]["business_name"] == "JOES PIZZA LLC"


def test_search_businesses_empty_results(client):
    """
    GET /businesses/search?name=zzzzz should return
    200 with empty results when nothing matches.
    """
    with patch("api.query", return_value=[]):
        response = client.get("/businesses/search?name=zzzzz")

    assert response.status_code == 200
    data = response.json()
    assert data["results"] == []
    assert data["count"] == 0


def test_search_businesses_name_too_short(client):
    """
    GET /businesses/search?name=a should return 422
    because our endpoint requires min_length=2.
    422 is FastAPI's validation error status code.
    """
    response = client.get("/businesses/search?name=a")
    assert response.status_code == 422


def test_get_business_by_license_not_found(client):
    """
    GET /businesses/INVALID should return 404
    when the license number doesn't exist.
    """
    with patch("api.query_one", return_value=None):
        response = client.get("/businesses/INVALID-LICENSE")

    assert response.status_code == 404


def test_get_business_by_license_found(client):
    """
    GET /businesses/123456 should return 200 with
    business details and associated anomalies.
    """
    fake_business = {
        "id": 1,
        "license_number": "123456",
        "business_name": "JOES PIZZA LLC",
        "license_status": "Active",
        "borough": "Brooklyn",
    }

    with patch("api.query_one", return_value=fake_business):
        with patch("api.query", return_value=[]):
            response = client.get("/businesses/123456")

    assert response.status_code == 200
    data = response.json()
    assert data["business"]["license_number"] == "123456"
    assert data["business"]["business_name"] == "JOES PIZZA LLC"
    assert data["anomalies"] == []


# -------------------------------------------------------
# SECTION 6: Entity Endpoint Tests
# -------------------------------------------------------

def test_get_entity_not_found(client):
    """
    GET /entities/INVALID should return 404
    when the DOS ID doesn't exist.
    """
    with patch("api.query_one", return_value=None):
        response = client.get("/entities/INVALID-DOS-ID")

    assert response.status_code == 404


def test_get_entity_found(client):
    """
    GET /entities/987654 should return 200 with
    entity details and associated anomalies.
    """
    fake_entity = {
        "id": 1,
        "dos_id": "987654",
        "current_entity_name": "JOES PIZZA LIMITED LIABILITY",
        "entity_type": "LLC",
        "date_of_dissolution": "2021-06-01",
    }

    with patch("api.query_one", return_value=fake_entity):
        with patch("api.query", return_value=[]):
            response = client.get("/entities/987654")

    assert response.status_code == 200
    data = response.json()
    assert data["entity"]["dos_id"] == "987654"
    assert data["anomalies"] == []