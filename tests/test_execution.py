"""
test_execution.py

Tests for execution.py covering:
- DB population checks
- Anomaly flag computation
- Anomaly writing to Postgres

unnittest.mock is used to avoid hitting real GCS/Postgres/Beam during tests
"""

import pytest
from datetime import date
from unittest.mock import patch, MagicMock, mock_open
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from execution import(
    tables_are_populated,
    create_tables,
    write_anomalies,
    compute_anomaly_flags,
    years_since
)

# Helper - Fake DB Connection
# Rather than connecting to a real Postgres database,
# create a fake connection using MagicMock.
# MagicMock automatically creates fake versions of any method you call on it, such as .cursor(), .commit()
def make_mock_conn(fetchone_return=None, fetchall_return=None):
    """
    Build a fake psycopg2 connection that returns whatever we tell it to.

    fetchone_return: what cur.fetchone() returns
    fetchall_return: what cur.fetchall() returns
    """
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = fetchone_return
    mock_cur.fetchall.return_value = fetchall_return or []

    # The cursor is used as a context manager: 'with conn.cursor() s cur'
    # So we need __enter__ to return our fake cursor
    mock_cur.__enter__ = lambda s: mock_cur
    mock_cur.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur

    return mock_conn, mock_cur

def test_tables_are_populated_returns_false_when_empty():
    """
    When both tables have 0 rows, tables_are_populated()
    should return False so the pipeline runs.
    """
    # fetchone returns (0,) — simulating COUNT(*) = 0
    mock_conn, _ = make_mock_conn(fetchone_return=(0,))

    # patch("execution.get_conn") replaces the real get_conn()
    # in execution.py with one that returns our fake connection.
    # This is the core mocking pattern — we intercept the call
    # before it reaches real Postgres.
    with patch("execution.get_conn", return_value=mock_conn):
        result = tables_are_populated()

    assert result is False


def test_tables_are_populated_returns_true_when_populated():
    """
    When both tables have rows, tables_are_populated()
    should return True so the pipeline is skipped.
    """
    # fetchone returns (100,) — simulating COUNT(*) = 100
    mock_conn, _ = make_mock_conn(fetchone_return=(100,))

    with patch("execution.get_conn", return_value=mock_conn):
        result = tables_are_populated()

    assert result is True



# create_tables() Tests

def test_create_tables_reads_sql_and_executes():
    """
    create_tables() should:
    1. Open database.sql
    2. Read its contents
    3. Execute the SQL against Postgres
    4. Commit the transaction
    """
    fake_sql      = "CREATE TABLE IF NOT EXISTS test (id SERIAL);"
    mock_conn, mock_cur = make_mock_conn()

    # mock_open is a special mock for file operations.
    # It simulates open() returning our fake_sql string
    # without touching the real filesystem.
    with patch("builtins.open", mock_open(read_data=fake_sql)):
        with patch("execution.get_conn", return_value=mock_conn):
            create_tables()

    # Verify the SQL was actually executed
    mock_cur.execute.assert_called_once_with(fake_sql)

    # Verify commit was called — without commit nothing is saved
    mock_conn.commit.assert_called_once()



# years_since() Tests


def test_years_since_recent_date():
    """
    A date from 1 year ago should return approximately 1.0.
    We use a range check because the exact value depends
    on when the test runs.
    """
    from datetime import timedelta
    one_year_ago = date.today().replace(year=date.today().year - 1)
    result       = years_since(one_year_ago)

    # Should be between 0.9 and 1.1 years
    assert 0.9 <= result <= 1.1


def test_years_since_old_date():
    """
    A date from 10 years ago should return approximately 10.0.
    """
    ten_years_ago = date.today().replace(year=date.today().year - 10)
    result        = years_since(ten_years_ago)

    assert 9.9 <= result <= 10.1



# compute_anomaly_flags() Tests
#
# We use type() to create lightweight fake objects that
# behave like our Pydantic models but don't need the
# full model machinery. This is the same pattern used
# in execution.py's fuzzy matching section.

def make_nyc(**kwargs):
    """Create a fake NYC business object with given attributes."""
    defaults = {
        "license_status":        "Active",
        "initial_issuance_date": date(2020, 1, 1),
        "expiration_date":       date(2025, 1, 1),
        "zip_code":              "10001",
    }
    defaults.update(kwargs)
    return type("NYC", (), defaults)()


def make_nys(**kwargs):
    """Create a fake NYS entity object with given attributes."""
    defaults = {
        "date_of_formation":   date(2015, 1, 1),
        "date_of_dissolution": None,
        "zip_code":            "10001",
    }
    defaults.update(kwargs)
    return type("NYS", (), defaults)()


def test_flag_dissolved_when_active_license_and_dissolved_entity():
    """
    flag_license_active_entity_dissolved should be True
    when the NYC license is Active but the NYS entity
    has a dissolution date.
    """
    nyc = make_nyc(license_status="Active")
    nys = make_nys(date_of_dissolution=date(2021, 6, 1))

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_license_active_entity_dissolved"] is True
    assert flags["has_anomaly"] is True


def test_flag_dissolved_false_when_entity_active():
    """
    flag_license_active_entity_dissolved should be False
    when the NYS entity has no dissolution date.
    """
    nyc = make_nyc(license_status="Active")
    nys = make_nys(date_of_dissolution=None)

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_license_active_entity_dissolved"] is False


def test_flag_predates_formation():
    """
    flag_license_predates_formation should be True when
    the NYC license was issued before the NYS entity formed.
    Joe's Pizza got a license in 2010 but the LLC wasn't
    registered until 2015 — that's a red flag.
    """
    nyc = make_nyc(initial_issuance_date=date(2010, 1, 1))
    nys = make_nys(date_of_formation=date(2015, 1, 1))

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_license_predates_formation"] is True
    assert flags["has_anomaly"] is True


def test_flag_predates_formation_false_when_normal():
    """
    flag_license_predates_formation should be False when
    the entity was formed before the license was issued.
    """
    nyc = make_nyc(initial_issuance_date=date(2018, 1, 1))
    nys = make_nys(date_of_formation=date(2015, 1, 1))

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_license_predates_formation"] is False


def test_flag_address_mismatch():
    """
    flag_address_mismatch should be True when the zip codes
    on the license and the registered entity don't match.
    """
    nyc = make_nyc(zip_code="10001")
    nys = make_nys(zip_code="11201")

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_address_mismatch"] is True


def test_flag_address_match():
    """
    flag_address_mismatch should be False when zip codes match.
    """
    nyc = make_nyc(zip_code="10001")
    nys = make_nys(zip_code="10001")

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_address_mismatch"] is False


def test_no_anomaly_when_everything_clean():
    """
    has_anomaly should be False when all flags are False.
    A perfectly clean business with matching records.
    """
    nyc = make_nyc(
        license_status="Active",
        initial_issuance_date=date(2018, 1, 1),
        zip_code="10001",
    )
    nys = make_nys(
        date_of_formation=date(2015, 1, 1),
        date_of_dissolution=None,
        zip_code="10001",
    )

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["has_anomaly"] is False



#  write_anomalies() Tests

def test_write_anomalies_skips_when_empty():
    """
    write_anomalies([]) should return early without
    touching the database at all.
    """
    with patch("execution.get_conn") as mock_get_conn:
        write_anomalies([])

    # get_conn should never have been called
    mock_get_conn.assert_not_called()


def test_write_anomalies_inserts_records():
    """
    write_anomalies() should insert one row per anomaly
    and commit the transaction.
    """
    mock_conn, mock_cur = make_mock_conn()

    anomalies = [{
        "nyc_business_id":                    1,
        "nys_entity_id":                      2,
        "match_score":                        92.5,
        "flag_license_active_entity_dissolved": True,
        "flag_license_predates_formation":    False,
        "flag_entity_dormant":                False,
        "flag_address_mismatch":              False,
        "has_anomaly":                        True,
    }]

    with patch("execution.get_conn", return_value=mock_conn):
        write_anomalies(anomalies)

    # Verify one INSERT was executed
    assert mock_cur.execute.call_count == 1

    # Verify commit was called
    mock_conn.commit.assert_called_once()


def test_write_anomalies_inserts_multiple_records():
    """
    write_anomalies() should insert ALL anomalies,
    not just the first one.
    """
    mock_conn, mock_cur = make_mock_conn()

    anomalies = [
        {
            "nyc_business_id":                    1,
            "nys_entity_id":                      2,
            "match_score":                        92.5,
            "flag_license_active_entity_dissolved": True,
            "flag_license_predates_formation":    False,
            "flag_entity_dormant":                False,
            "flag_address_mismatch":              False,
            "has_anomaly":                        True,
        },
        {
            "nyc_business_id":                    3,
            "nys_entity_id":                      4,
            "match_score":                        88.0,
            "flag_license_active_entity_dissolved": False,
            "flag_license_predates_formation":    True,
            "flag_entity_dormant":                False,
            "flag_address_mismatch":              False,
            "has_anomaly":                        True,
        },
    ]

    with patch("execution.get_conn", return_value=mock_conn):
        write_anomalies(anomalies)

    # Two anomalies = two INSERT statements
    assert mock_cur.execute.call_count == 2
    mock_conn.commit.assert_called_once()