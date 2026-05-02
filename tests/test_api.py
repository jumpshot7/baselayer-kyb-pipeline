"""
test_api.py

Tests for api.py using FastAPI's TestClient.

Key differences from original:
- nys_corp_entities only has 4 columns: dos_id, current_entity_name,
  initial_dos_filing_date, zip_code
- Removed fields: entity_type, dos_process_name, date_of_dissolution,
  date_of_formation, dba_trade_name, county, jurisdiction
- flag_license_active_entity_dissolved is always False
- get_anomalies uses initial_dos_filing_date not date_of_formation
- search_businesses only searches business_name (no dba_trade_name)
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

@pytest.fixture
def client():
    return TestClient(app)


# -------------------------------------------------------
# SECTION 2: Health Check
# -------------------------------------------------------

def test_health_returns_200(client):
    """GET /health should return 200 with status ok and anomaly count."""
    with patch("api.query_one", return_value={"count": 42}):
        response = client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["anomaly_count"] == 42


def test_health_returns_500_on_db_error(client):
    """GET /health should return 500 if the DB is down."""
    with patch("api.query_one", side_effect=Exception("DB connection failed")):
        response = client.get("/health")

    assert response.status_code == 500


def test_health_returns_zero_when_no_anomalies(client):
    """GET /health should return anomaly_count 0 when table is empty."""
    with patch("api.query_one", return_value={"count": 0}):
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["anomaly_count"] == 0


# -------------------------------------------------------
# SECTION 3: Anomaly Summary
# -------------------------------------------------------

def test_get_anomaly_summary(client):
    """GET /anomalies/summary should return counts for each flag type."""
    fake_summary = {
        "total_anomalies": 15071,
        "flag_license_active_entity_dissolved": 0,  # always 0
        "flag_license_predates_formation": 6106,
        "flag_entity_dormant": 10326,
        "flag_address_mismatch": 0,
    }

    with patch("api.query_one", return_value=fake_summary):
        response = client.get("/anomalies/summary")

    assert response.status_code == 200
    data = response.json()
    assert data["total_anomalies"] == 15071
    assert data["flag_license_active_entity_dissolved"] == 0
    assert data["flag_license_predates_formation"] == 6106
    assert data["flag_entity_dormant"] == 10326


# -------------------------------------------------------
# SECTION 4: Anomaly List Endpoint
# -------------------------------------------------------

def make_fake_anomaly(id=1, score=100.0, predates=False, dormant=True, address=False):
    """Helper to build a fake anomaly row matching the stripped schema."""
    return {
        "id": id,
        "match_score": score,
        "flag_license_active_entity_dissolved": False,  # always False
        "flag_license_predates_formation": predates,
        "flag_entity_dormant": dormant,
        "flag_address_mismatch": address,
        "has_anomaly": True,
        "created_at": "2024-01-01T00:00:00",
        # NYC fields
        "license_number": f"LIC-{id}",
        "business_name": f"TEST BUSINESS {id}",
        "license_status": "Expired",
        "license_type": "Business",
        "expiration_date": "2020-01-01",
        "borough": "Brooklyn",
        "nyc_zip": "11201",
        # NYS fields — stripped to 4 columns only
        "dos_id": f"DOS-{id}",
        "current_entity_name": f"TEST CORP {id}",
        "initial_dos_filing_date": "2015-01-01",
        "nys_zip": "10001",
    }


def test_get_anomalies_returns_200(client):
    """GET /anomalies should return 200 with a results list."""
    fake_anomalies = [make_fake_anomaly(1), make_fake_anomaly(2)]

    with patch("api.query", return_value=fake_anomalies):
        response = client.get("/anomalies")

    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    assert len(data["results"]) == 2
    assert data["count"] == 2


def test_get_anomalies_empty_results(client):
    """GET /anomalies should return 200 with empty list when no anomalies."""
    with patch("api.query", return_value=[]):
        response = client.get("/anomalies")

    assert response.status_code == 200
    data = response.json()
    assert data["results"] == []
    assert data["count"] == 0


def test_get_anomalies_pagination(client):
    """GET /anomalies should respect limit and offset params."""
    with patch("api.query", return_value=[]) as mock_query:
        response = client.get("/anomalies?limit=10&offset=50")

    assert response.status_code == 200


def test_get_anomalies_flag_filter_predates(client):
    """GET /anomalies?flag_predates=true should filter by predates flag."""
    with patch("api.query", return_value=[make_fake_anomaly(predates=True)]):
        response = client.get("/anomalies?flag_predates=true")

    assert response.status_code == 200
    assert len(response.json()["results"]) == 1


def test_get_anomalies_flag_filter_dormant(client):
    """GET /anomalies?flag_dormant=true should filter by dormant flag."""
    with patch("api.query", return_value=[make_fake_anomaly(dormant=True)]):
        response = client.get("/anomalies?flag_dormant=true")

    assert response.status_code == 200


def test_get_anomalies_no_nys_columns_that_dont_exist(client):
    """
    Anomaly results should NOT contain entity_type, dos_process_name,
    or date_of_dissolution — those columns don't exist in our
    stripped-down nys_corp_entities table.
    """
    fake_anomaly = make_fake_anomaly()
    with patch("api.query", return_value=[fake_anomaly]):
        response = client.get("/anomalies")

    data = response.json()
    result = data["results"][0]
    assert "entity_type" not in result
    assert "dos_process_name" not in result
    assert "date_of_dissolution" not in result


# -------------------------------------------------------
# SECTION 5: Single Anomaly Detail
# -------------------------------------------------------

def test_get_anomaly_by_id_found(client):
    """GET /anomalies/1 should return 200 with the full anomaly record."""
    fake_anomaly = {
        "id": 1,
        "match_score": 100.0,
        "has_anomaly": True,
        "flag_license_active_entity_dissolved": False,
        "flag_license_predates_formation": True,
        "flag_entity_dormant": False,
        "flag_address_mismatch": False,
        "nyc_business": {
            "business_name": "JOES PIZZA LLC",
            "license_number": "123456",
            "license_status": "Active",
            "zip_code": "10001",
        },
        "nys_entity": {
            "dos_id": "987654",
            "current_entity_name": "JOES PIZZA LIMITED LIABILITY",
            "initial_dos_filing_date": "2015-01-01",
            "zip_code": "10001",
        },
    }

    with patch("api.query_one", return_value=fake_anomaly):
        response = client.get("/anomalies/1")

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == 1
    assert data["match_score"] == 100.0
    assert data["flag_license_active_entity_dissolved"] is False


def test_get_anomaly_by_id_not_found(client):
    """GET /anomalies/99999 should return 404 when ID doesn't exist."""
    with patch("api.query_one", return_value=None):
        response = client.get("/anomalies/99999")

    assert response.status_code == 404


# -------------------------------------------------------
# SECTION 6: Dissolved & Predates Endpoints
# -------------------------------------------------------

def test_get_dissolved_anomalies(client):
    """
    GET /anomalies/dissolved returns records where
    flag_license_active_entity_dissolved is True.
    Note: in practice this is always 0 with current dataset.
    """
    fake_results = [{
        "business_name": "JOES PIZZA LLC",
        "license_number": "123456",
        "license_status": "Active",
        "expiration_date": "2025-01-01",
        "borough": "Brooklyn",
        "current_entity_name": "JOES PIZZA LIMITED LIABILITY",
        "match_score": 92.5,
    }]

    with patch("api.query", return_value=fake_results):
        response = client.get("/anomalies/dissolved")

    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 1


def test_get_predates_anomalies(client):
    """
    GET /anomalies/predates should return records where
    license was issued before entity formation.
    """
    fake_results = [{
        "business_name": "MARIOS DELI",
        "license_number": "654321",
        "initial_issuance_date": "2010-01-01",
        "current_entity_name": "MARIO DELI INC",
        "date_of_formation": "2015-01-01",
        "match_score": 88.0,
    }]

    with patch("api.query", return_value=fake_results):
        response = client.get("/anomalies/predates")

    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 1
    assert data["results"][0]["business_name"] == "MARIOS DELI"


# -------------------------------------------------------
# SECTION 7: Borough Endpoint
# -------------------------------------------------------

def test_get_anomalies_by_borough_returns_count(client):
    """
    GET /anomalies/by-borough/Brooklyn should return
    the real count from the DB, not len(results).
    """
    fake_count = {"count": 1829}
    fake_results = [make_fake_anomaly()]

    with patch("api.query_one", return_value=fake_count):
        with patch("api.query", return_value=fake_results):
            response = client.get("/anomalies/by-borough/Brooklyn?limit=1")

    assert response.status_code == 200
    data = response.json()
    assert data["borough"] == "Brooklyn"
    assert data["count"] == 1829


def test_get_anomalies_by_borough_case_insensitive(client):
    """Borough matching should be case insensitive."""
    fake_count = {"count": 500}
    with patch("api.query_one", return_value=fake_count):
        with patch("api.query", return_value=[]):
            response = client.get("/anomalies/by-borough/brooklyn")

    assert response.status_code == 200


# -------------------------------------------------------
# SECTION 8: Business Search
# -------------------------------------------------------

def test_search_businesses_returns_results(client):
    """
    GET /businesses/search?name=pizza should return matching businesses.
    Note: dba_trade_name is NOT included — column doesn't exist.
    """
    fake_results = [{
        "id": 1,
        "license_number": "123456",
        "business_name": "JOES PIZZA LLC",
        "license_status": "Active",
        "license_type": "Retail",
        "business_category": "Food",
        "expiration_date": "2025-01-01",
        "borough": "Brooklyn",
        "zip_code": "11201",
    }]

    with patch("api.query", return_value=fake_results):
        response = client.get("/businesses/search?name=pizza")

    assert response.status_code == 200
    data = response.json()
    assert data["query"] == "pizza"
    assert len(data["results"]) == 1
    assert data["results"][0]["business_name"] == "JOES PIZZA LLC"
    # Confirm dba_trade_name is not expected in results
    assert "dba_trade_name" not in data["results"][0]


def test_search_businesses_empty_results(client):
    """GET /businesses/search?name=zzzzz should return 200 with empty list."""
    with patch("api.query", return_value=[]):
        response = client.get("/businesses/search?name=zzzzz")

    assert response.status_code == 200
    data = response.json()
    assert data["results"] == []
    assert data["count"] == 0


def test_search_businesses_name_too_short(client):
    """GET /businesses/search?name=a should return 422 (min_length=2)."""
    response = client.get("/businesses/search?name=a")
    assert response.status_code == 422


def test_search_businesses_name_missing(client):
    """GET /businesses/search with no name param should return 422."""
    response = client.get("/businesses/search")
    assert response.status_code == 422


# -------------------------------------------------------
# SECTION 9: Business by License Number
# -------------------------------------------------------

def test_get_business_by_license_found(client):
    """GET /businesses/123456 should return business details and anomalies."""
    fake_business = {
        "id": 1,
        "license_number": "123456",
        "business_name": "JOES PIZZA LLC",
        "license_status": "Active",
        "borough": "Brooklyn",
        "zip_code": "11201",
    }
    fake_anomalies = [{
        "match_score": 100.0,
        "has_anomaly": True,
        "flag_license_active_entity_dissolved": False,
        "flag_license_predates_formation": True,
        "flag_entity_dormant": False,
        "flag_address_mismatch": False,
        "current_entity_name": "JOES PIZZA LIMITED LIABILITY",
        "initial_dos_filing_date": "2020-01-01",
    }]

    with patch("api.query_one", return_value=fake_business):
        with patch("api.query", return_value=fake_anomalies):
            response = client.get("/businesses/123456")

    assert response.status_code == 200
    data = response.json()
    assert data["business"]["license_number"] == "123456"
    assert len(data["anomalies"]) == 1


def test_get_business_by_license_not_found(client):
    """GET /businesses/INVALID should return 404."""
    with patch("api.query_one", return_value=None):
        response = client.get("/businesses/INVALID-LICENSE")

    assert response.status_code == 404


def test_get_business_by_license_no_anomalies(client):
    """GET /businesses/123456 should return empty anomalies list if none."""
    fake_business = {"id": 1, "license_number": "123456", "business_name": "CLEAN BIZ"}

    with patch("api.query_one", return_value=fake_business):
        with patch("api.query", return_value=[]):
            response = client.get("/businesses/123456")

    assert response.status_code == 200
    assert response.json()["anomalies"] == []


# -------------------------------------------------------
# SECTION 10: Entity Endpoints
# -------------------------------------------------------

def test_get_entity_found(client):
    """
    GET /entities/987654 should return entity details.
    Only 4 columns available: dos_id, current_entity_name,
    initial_dos_filing_date, zip_code.
    """
    fake_entity = {
        "id": 1,
        "dos_id": "987654",
        "current_entity_name": "JOES PIZZA LIMITED LIABILITY",
        "initial_dos_filing_date": "2015-01-01",
        "zip_code": "10001",
    }

    with patch("api.query_one", return_value=fake_entity):
        with patch("api.query", return_value=[]):
            response = client.get("/entities/987654")

    assert response.status_code == 200
    data = response.json()
    assert data["entity"]["dos_id"] == "987654"
    assert data["entity"]["current_entity_name"] == "JOES PIZZA LIMITED LIABILITY"
    assert data["anomalies"] == []


def test_get_entity_not_found(client):
    """GET /entities/INVALID should return 404."""
    with patch("api.query_one", return_value=None):
        response = client.get("/entities/INVALID-DOS-ID")

    assert response.status_code == 404


def test_get_entity_with_anomalies(client):
    """GET /entities/987654 should also return associated anomalies."""
    fake_entity = {
        "id": 1,
        "dos_id": "987654",
        "current_entity_name": "TEST CORP",
        "initial_dos_filing_date": "2015-01-01",
        "zip_code": "10001",
    }
    fake_anomalies = [{
        "match_score": 95.0,
        "has_anomaly": True,
        "flag_license_active_entity_dissolved": False,
        "flag_license_predates_formation": False,
        "flag_entity_dormant": True,
        "flag_address_mismatch": False,
        "business_name": "TEST BUSINESS LLC",
        "license_number": "LIC-123",
        "license_status": "Expired",
    }]

    with patch("api.query_one", return_value=fake_entity):
        with patch("api.query", return_value=fake_anomalies):
            response = client.get("/entities/987654")

    assert response.status_code == 200
    data = response.json()
    assert len(data["anomalies"]) == 1
    assert data["anomalies"][0]["flag_entity_dormant"] is True