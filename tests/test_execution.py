"""
test_execution.py

Tests for execution.py covering:
- DB population checks
- Table creation
- Anomaly flag computation (matches actual logic in execution.py)
- Writing anomalies to Postgres

Key differences from original:
- flag_license_active_entity_dissolved is ALWAYS False (hardcoded)
  because the NYS dataset only contains active entities
- flag_entity_dormant uses nyc.license_status IN DEAD_LICENSE_STATUSES
  AND years_since(nys.initial_dos_filing_date) > 3
- flag_predates uses nyc.initial_issuance_date < nys.initial_dos_filing_date
- flag_address uses nyc.zip_code[:5] != nys.zip_code[:5]
- NysCorpEntity uses initial_dos_filing_date (not date_of_formation)
"""

import pytest
from datetime import date
from unittest.mock import patch, MagicMock, mock_open
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from execution import (
    tables_are_populated,
    create_tables,
    write_anomalies,
    compute_anomaly_flags,
    years_since,
    DEAD_LICENSE_STATUSES,
    DORMANT_YEARS_THRESHOLD,
    MATCH_THRESHOLD,
)


# -------------------------------------------------------
# Helper: Fake DB Connection
# -------------------------------------------------------

def make_mock_conn(fetchone_return=None, fetchall_return=None):
    """
    Build a fake psycopg2 connection that returns whatever we tell it to.
    """
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = fetchone_return
    mock_cur.fetchall.return_value = fetchall_return or []
    mock_cur.__enter__ = lambda s: mock_cur
    mock_cur.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cur

    return mock_conn, mock_cur


def make_nyc(**kwargs):
    """Create a fake NYC business object with given attributes."""
    defaults = {
        "license_status":        "Active",
        "initial_issuance_date": date(2020, 1, 1),
        "zip_code":              "10001",
    }
    defaults.update(kwargs)
    return type("NYC", (), defaults)()


def make_nys(**kwargs):
    """Create a fake NYS entity object with given attributes."""
    defaults = {
        "initial_dos_filing_date": date(2015, 1, 1),
        "zip_code":                "10001",
    }
    defaults.update(kwargs)
    return type("NYS", (), defaults)()


# -------------------------------------------------------
# SECTION 1: tables_are_populated()
# -------------------------------------------------------

def test_tables_are_populated_returns_false_when_empty():
    """
    When both tables have 0 rows, tables_are_populated()
    should return False so the pipeline runs.
    """
    mock_conn, _ = make_mock_conn(fetchone_return=(0,))

    with patch("execution.get_conn", return_value=mock_conn):
        result = tables_are_populated()

    assert result is False


def test_tables_are_populated_returns_true_when_populated():
    """
    When both tables have rows, tables_are_populated()
    should return True so the pipeline is skipped.
    """
    mock_conn, _ = make_mock_conn(fetchone_return=(100,))

    with patch("execution.get_conn", return_value=mock_conn):
        result = tables_are_populated()

    assert result is True


def test_tables_are_populated_false_when_only_nyc_populated():
    """
    If only one table has rows, pipeline should still run.
    Both tables need rows to skip the pipeline.
    """
    mock_conn, mock_cur = make_mock_conn()
    # First COUNT returns 100 (NYC populated), second returns 0 (NYS empty)
    mock_cur.fetchone.side_effect = [(100,), (0,)]

    with patch("execution.get_conn", return_value=mock_conn):
        result = tables_are_populated()

    assert result is False


# -------------------------------------------------------
# SECTION 2: create_tables()
# -------------------------------------------------------

def test_create_tables_reads_sql_and_executes():
    """
    create_tables() should open database.sql, read its contents,
    execute the SQL, and commit the transaction.
    """
    fake_sql = "CREATE TABLE IF NOT EXISTS test (id SERIAL);"
    mock_conn, mock_cur = make_mock_conn()

    with patch("builtins.open", mock_open(read_data=fake_sql)):
        with patch("execution.get_conn", return_value=mock_conn):
            create_tables()

    mock_cur.execute.assert_called_once_with(fake_sql)
    mock_conn.commit.assert_called_once()


# -------------------------------------------------------
# SECTION 3: years_since()
# -------------------------------------------------------

def test_years_since_one_year_ago():
    """A date 1 year ago should return approximately 1.0."""
    one_year_ago = date.today().replace(year=date.today().year - 1)
    result = years_since(one_year_ago)
    assert 0.9 <= result <= 1.1


def test_years_since_ten_years_ago():
    """A date 10 years ago should return approximately 10.0."""
    ten_years_ago = date.today().replace(year=date.today().year - 10)
    result = years_since(ten_years_ago)
    assert 9.9 <= result <= 10.1


def test_years_since_today():
    """Today should return approximately 0."""
    result = years_since(date.today())
    assert 0.0 <= result < 0.01


# -------------------------------------------------------
# SECTION 4: compute_anomaly_flags()
# -------------------------------------------------------

def test_flag_dissolved_always_false():
    """
    flag_license_active_entity_dissolved is ALWAYS False.
    The NYS Active Corporations dataset only contains active
    entities — a dissolved dataset would be needed to compute this.
    """
    nyc = make_nyc(license_status="Active")
    nys = make_nys()

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_license_active_entity_dissolved"] is False


def test_flag_predates_formation_true():
    """
    flag_license_predates_formation should be True when the NYC
    license was issued before the NYS entity was formed.
    """
    nyc = make_nyc(initial_issuance_date=date(2010, 1, 1))
    nys = make_nys(initial_dos_filing_date=date(2015, 1, 1))

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_license_predates_formation"] is True
    assert flags["has_anomaly"] is True


def test_flag_predates_formation_false_when_entity_formed_first():
    """
    flag_license_predates_formation should be False when the
    entity was formed before the license was issued.
    """
    nyc = make_nyc(initial_issuance_date=date(2018, 1, 1))
    nys = make_nys(initial_dos_filing_date=date(2015, 1, 1))

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_license_predates_formation"] is False


def test_flag_predates_formation_false_when_same_date():
    """
    flag_license_predates_formation should be False when the
    license and formation dates are the same.
    """
    nyc = make_nyc(initial_issuance_date=date(2015, 1, 1))
    nys = make_nys(initial_dos_filing_date=date(2015, 1, 1))

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_license_predates_formation"] is False


def test_flag_predates_formation_false_when_dates_missing():
    """
    flag_license_predates_formation should be False when either
    date is None — can't compare missing data.
    """
    nyc = make_nyc(initial_issuance_date=None)
    nys = make_nys(initial_dos_filing_date=date(2015, 1, 1))

    flags = compute_anomaly_flags(nyc, nys)
    assert flags["flag_license_predates_formation"] is False

    nyc2 = make_nyc(initial_issuance_date=date(2010, 1, 1))
    nys2 = make_nys(initial_dos_filing_date=None)

    flags2 = compute_anomaly_flags(nyc2, nys2)
    assert flags2["flag_license_predates_formation"] is False


def test_flag_entity_dormant_true_expired_license():
    """
    flag_entity_dormant should be True when the NYC license is
    Expired AND the NYS entity has been active for more than
    DORMANT_YEARS_THRESHOLD years.
    """
    old_date = date.today().replace(year=date.today().year - (DORMANT_YEARS_THRESHOLD + 1))
    nyc = make_nyc(license_status="Expired")
    nys = make_nys(initial_dos_filing_date=old_date)

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_entity_dormant"] is True
    assert flags["has_anomaly"] is True


def test_flag_entity_dormant_true_surrendered_license():
    """
    flag_entity_dormant should be True for Surrendered licenses too.
    """
    old_date = date.today().replace(year=date.today().year - (DORMANT_YEARS_THRESHOLD + 1))
    nyc = make_nyc(license_status="Surrendered")
    nys = make_nys(initial_dos_filing_date=old_date)

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_entity_dormant"] is True


def test_flag_entity_dormant_false_active_license():
    """
    flag_entity_dormant should be False when the license is Active
    even if the entity is old.
    """
    old_date = date.today().replace(year=date.today().year - 10)
    nyc = make_nyc(license_status="Active")
    nys = make_nys(initial_dos_filing_date=old_date)

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_entity_dormant"] is False


def test_flag_entity_dormant_false_entity_too_young():
    """
    flag_entity_dormant should be False when the entity is younger
    than DORMANT_YEARS_THRESHOLD years, even with a dead license.
    """
    recent_date = date.today().replace(year=date.today().year - 1)
    nyc = make_nyc(license_status="Expired")
    nys = make_nys(initial_dos_filing_date=recent_date)

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_entity_dormant"] is False


def test_flag_address_mismatch_true():
    """
    flag_address_mismatch should be True when the first 5 digits
    of the zip codes don't match.
    """
    nyc = make_nyc(zip_code="10001")
    nys = make_nys(zip_code="11201")

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_address_mismatch"] is True
    assert flags["has_anomaly"] is True


def test_flag_address_mismatch_false_when_zips_match():
    """
    flag_address_mismatch should be False when zip codes match.
    """
    nyc = make_nyc(zip_code="10001")
    nys = make_nys(zip_code="10001")

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_address_mismatch"] is False


def test_flag_address_mismatch_handles_zip_plus_four():
    """
    flag_address_mismatch should only compare the first 5 digits,
    so ZIP+4 format (10001-1234) should match plain 10001.
    """
    nyc = make_nyc(zip_code="10001-1234")
    nys = make_nys(zip_code="10001")

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_address_mismatch"] is False


def test_flag_address_mismatch_false_when_zip_missing():
    """
    flag_address_mismatch should be False when either zip is None.
    """
    nyc = make_nyc(zip_code=None)
    nys = make_nys(zip_code="10001")

    flags = compute_anomaly_flags(nyc, nys)
    assert flags["flag_address_mismatch"] is False

    nyc2 = make_nyc(zip_code="10001")
    nys2 = make_nys(zip_code=None)

    flags2 = compute_anomaly_flags(nyc2, nys2)
    assert flags2["flag_address_mismatch"] is False


def test_no_anomaly_when_everything_clean():
    """
    has_anomaly should be False when all flags are False.
    """
    nyc = make_nyc(
        license_status="Active",
        initial_issuance_date=date(2018, 1, 1),
        zip_code="10001",
    )
    nys = make_nys(
        initial_dos_filing_date=date(2015, 1, 1),
        zip_code="10001",
    )

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_license_active_entity_dissolved"] is False
    assert flags["flag_license_predates_formation"] is False
    assert flags["flag_entity_dormant"] is False
    assert flags["flag_address_mismatch"] is False
    assert flags["has_anomaly"] is False


def test_multiple_flags_can_fire_simultaneously():
    """
    Multiple flags can be True at the same time for the same pair.
    """
    old_date = date.today().replace(year=date.today().year - (DORMANT_YEARS_THRESHOLD + 1))
    nyc = make_nyc(
        license_status="Expired",
        initial_issuance_date=date(2010, 1, 1),
        zip_code="10001",
    )
    nys = make_nys(
        initial_dos_filing_date=old_date,
        zip_code="11201",
    )

    flags = compute_anomaly_flags(nyc, nys)

    assert flags["flag_entity_dormant"] is True
    assert flags["flag_address_mismatch"] is True
    assert flags["has_anomaly"] is True


# -------------------------------------------------------
# SECTION 5: Constants
# -------------------------------------------------------

def test_dead_license_statuses_contains_expected_values():
    """DEAD_LICENSE_STATUSES should include Expired and Surrendered."""
    assert "Expired" in DEAD_LICENSE_STATUSES
    assert "Surrendered" in DEAD_LICENSE_STATUSES
    assert "Active" not in DEAD_LICENSE_STATUSES


def test_match_threshold_is_reasonable():
    """MATCH_THRESHOLD should be between 80 and 100."""
    assert 80.0 <= MATCH_THRESHOLD <= 100.0


def test_dormant_years_threshold_is_positive():
    """DORMANT_YEARS_THRESHOLD should be a positive number."""
    assert DORMANT_YEARS_THRESHOLD > 0


# -------------------------------------------------------
# SECTION 6: write_anomalies()
# -------------------------------------------------------

def test_write_anomalies_skips_when_empty():
    """
    write_anomalies([]) should return early without
    touching the database.
    """
    with patch("execution.get_conn") as mock_get_conn:
        write_anomalies([])

    mock_get_conn.assert_not_called()


def test_write_anomalies_skips_clean_pairs():
    """
    write_anomalies() should skip pairs where has_anomaly is False.
    Only anomalous pairs get written to the DB.
    """
    mock_conn, mock_cur = make_mock_conn()

    anomalies = [
        {
            "nyc_business_id": 1,
            "nys_entity_id": 2,
            "match_score": 92.5,
            "flag_license_active_entity_dissolved": False,
            "flag_license_predates_formation": False,
            "flag_entity_dormant": False,
            "flag_address_mismatch": False,
            "has_anomaly": False,  # clean pair — should be skipped
        }
    ]

    with patch("execution.get_conn", return_value=mock_conn):
        write_anomalies(anomalies)

    mock_get_conn = patch("execution.get_conn", return_value=mock_conn)
    assert mock_cur.execute.call_count == 0


def test_write_anomalies_inserts_anomalous_records():
    """
    write_anomalies() should insert rows where has_anomaly is True
    and commit the transaction.
    """
    mock_conn, mock_cur = make_mock_conn()

    anomalies = [{
        "nyc_business_id": 1,
        "nys_entity_id": 2,
        "match_score": 92.5,
        "flag_license_active_entity_dissolved": False,
        "flag_license_predates_formation": True,
        "flag_entity_dormant": False,
        "flag_address_mismatch": False,
        "has_anomaly": True,
    }]

    with patch("execution.get_conn", return_value=mock_conn):
        write_anomalies(anomalies)

    assert mock_cur.execute.call_count == 1
    mock_conn.commit.assert_called_once()


def test_write_anomalies_inserts_multiple_records():
    """
    write_anomalies() should insert ALL anomalous records.
    """
    mock_conn, mock_cur = make_mock_conn()

    anomalies = [
        {
            "nyc_business_id": 1, "nys_entity_id": 2, "match_score": 92.5,
            "flag_license_active_entity_dissolved": False,
            "flag_license_predates_formation": True,
            "flag_entity_dormant": False, "flag_address_mismatch": False,
            "has_anomaly": True,
        },
        {
            "nyc_business_id": 3, "nys_entity_id": 4, "match_score": 88.0,
            "flag_license_active_entity_dissolved": False,
            "flag_license_predates_formation": False,
            "flag_entity_dormant": True, "flag_address_mismatch": True,
            "has_anomaly": True,
        },
    ]

    with patch("execution.get_conn", return_value=mock_conn):
        write_anomalies(anomalies)

    assert mock_cur.execute.call_count == 2
    mock_conn.commit.assert_called_once()


def test_write_anomalies_mixed_filters_clean_pairs():
    """
    write_anomalies() with a mix of clean and anomalous pairs
    should only insert the anomalous ones.
    """
    mock_conn, mock_cur = make_mock_conn()

    anomalies = [
        {
            "nyc_business_id": 1, "nys_entity_id": 2, "match_score": 100.0,
            "flag_license_active_entity_dissolved": False,
            "flag_license_predates_formation": False,
            "flag_entity_dormant": False, "flag_address_mismatch": False,
            "has_anomaly": False,  # clean — skip
        },
        {
            "nyc_business_id": 3, "nys_entity_id": 4, "match_score": 88.0,
            "flag_license_active_entity_dissolved": False,
            "flag_license_predates_formation": True,
            "flag_entity_dormant": False, "flag_address_mismatch": False,
            "has_anomaly": True,  # anomalous — insert
        },
    ]

    with patch("execution.get_conn", return_value=mock_conn):
        write_anomalies(anomalies)

    assert mock_cur.execute.call_count == 1