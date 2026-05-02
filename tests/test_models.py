"""
test_models.py

Tests for models.py covering:
- NycDcaBusiness model validation
- NysCorpEntity model validation  
- parse_nyc_record() and parse_nys_record() helpers

Key difference from original: NysCorpEntity uses snake_case aliases
(dos_id, current_entity_name, initial_dos_filing_date, dos_process_zip)
because data comes from the Socrata /resource/ API endpoint, not the
bulk CSV export which uses title-case headers.
"""

import pytest
from datetime import date
from models import (
    NycDcaBusiness,
    NysCorpEntity,
    parse_nyc_record,
    parse_nys_record,
)


# -------------------------------------------------------
# SECTION 1: Fixtures
# -------------------------------------------------------

@pytest.fixture
def valid_nyc_row():
    """
    Valid NYC DCA CSV row. Uses title-case aliases matching
    the bulk CSV export headers.
    """
    return {
        "License Number":       "1234567-DCA",
        "Business Name":        "TEST BUSINESS LLC",
        "Business Unique ID":   "BU-9999",
        "Business Category":    "Retail Food",
        "License Type":         "Business",
        "License Status":       "Active",
        "Initial Issuance Date": "01/15/2020",
        "Expiration Date":      "01/15/2025",
        "Contact Phone":        "2125551234",
        "Building Number":      "123",
        "Street1":              "MAIN ST",
        "City":                 "New York",
        "State":                "NY",
        "ZIP Code":             "10001",
        "Borough":              "Manhattan",
        "Latitude":             "40.7218",
        "Longitude":            "-74.0060",
    }


@pytest.fixture
def valid_nys_row():
    """
    Valid NYS corporation row. Uses snake_case aliases matching
    the Socrata /resource/ API endpoint response.
    Note: zip comes from dos_process_zip field.
    """
    return {
        "dos_id":                   "987654",
        "current_entity_name":      "TEST CORP INC",
        "initial_dos_filing_date":  "2015-05-20",
        "dos_process_zip":          "10001",
    }


# -------------------------------------------------------
# SECTION 2: NycDcaBusiness Model Tests
# -------------------------------------------------------

def test_nyc_model_valid(valid_nyc_row):
    """
    A valid NYC row should parse correctly into a NycDcaBusiness,
    including date coercion and float coercion.
    """
    record = NycDcaBusiness.model_validate(valid_nyc_row)

    assert record.license_number == "1234567-DCA"
    assert record.business_name == "TEST BUSINESS LLC"
    assert record.business_category == "Retail Food"
    assert record.license_status == "Active"
    assert record.borough == "Manhattan"
    assert record.zip_code == "10001"

    # Date coercion
    assert record.initial_issuance_date == date(2020, 1, 15)
    assert record.expiration_date == date(2025, 1, 15)

    # Float coercion
    assert record.latitude == 40.7218
    assert record.longitude == -74.0060


def test_nyc_model_missing_license_number():
    """
    License Number is required — missing it should return None
    from parse_nyc_record().
    """
    invalid_row = {
        "Business Name": "TEST BUSINESS LLC",
    }
    result = parse_nyc_record(invalid_row)
    assert result is None


def test_nyc_model_missing_business_name():
    """
    Business Name is required — missing it should return None.
    """
    invalid_row = {
        "License Number": "1234567-DCA",
    }
    result = parse_nyc_record(invalid_row)
    assert result is None


def test_nyc_float_parsing_bad_values(valid_nyc_row):
    """
    Bad latitude/longitude values should become None
    instead of raising an exception.
    """
    valid_nyc_row["Latitude"] = "NOT_A_FLOAT"
    valid_nyc_row["Longitude"] = ""

    record = parse_nyc_record(valid_nyc_row)
    assert record is not None
    assert record.latitude is None
    assert record.longitude is None


def test_nyc_date_parsing_bad_value(valid_nyc_row):
    """
    An unparseable date should become None instead of crashing.
    """
    valid_nyc_row["Initial Issuance Date"] = "NOT-A-DATE"

    record = parse_nyc_record(valid_nyc_row)
    assert record is not None
    assert record.initial_issuance_date is None


def test_nyc_date_parsing_iso_format(valid_nyc_row):
    """
    ISO format dates (YYYY-MM-DD) should also parse correctly.
    """
    valid_nyc_row["Initial Issuance Date"] = "2020-01-15"

    record = parse_nyc_record(valid_nyc_row)
    assert record is not None
    assert record.initial_issuance_date == date(2020, 1, 15)


def test_nyc_date_parsing_datetime_format(valid_nyc_row):
    """
    Datetime format (YYYY-MM-DDTHH:MM:SS.ffffff) should also parse correctly.
    """
    valid_nyc_row["Initial Issuance Date"] = "2020-01-15T00:00:00.000000"

    record = parse_nyc_record(valid_nyc_row)
    assert record is not None
    assert record.initial_issuance_date == date(2020, 1, 15)


def test_nyc_optional_fields_can_be_empty(valid_nyc_row):
    """
    Optional fields like contact_phone, building_number, etc.
    should gracefully handle empty strings.
    """
    valid_nyc_row["Contact Phone"] = ""
    valid_nyc_row["Building Number"] = ""
    valid_nyc_row["Street1"] = ""

    record = parse_nyc_record(valid_nyc_row)
    assert record is not None
    # Empty strings stay as empty strings (not None) for text fields


# -------------------------------------------------------
# SECTION 3: NysCorpEntity Model Tests
# -------------------------------------------------------

def test_nys_model_valid(valid_nys_row):
    """
    A valid NYS row should parse correctly into a NysCorpEntity.
    Uses snake_case aliases from the Socrata /resource/ API.
    """
    record = NysCorpEntity.model_validate(valid_nys_row)

    assert record.dos_id == "987654"
    assert record.current_entity_name == "TEST CORP INC"
    assert record.initial_dos_filing_date == date(2015, 5, 20)
    assert record.zip_code == "10001"


def test_nys_model_missing_dos_id():
    """
    dos_id is required — missing it should return None.
    """
    invalid_row = {
        "current_entity_name": "TEST CORP INC",
        "initial_dos_filing_date": "2015-05-20",
        "dos_process_zip": "10001",
    }
    result = parse_nys_record(invalid_row)
    assert result is None


def test_nys_model_missing_entity_name():
    """
    current_entity_name is required — missing it should return None.
    """
    invalid_row = {
        "dos_id": "987654",
        "initial_dos_filing_date": "2015-05-20",
        "dos_process_zip": "10001",
    }
    result = parse_nys_record(invalid_row)
    assert result is None


def test_nys_model_optional_date_is_none():
    """
    initial_dos_filing_date is optional — missing it should
    still produce a valid record with None for that field.
    """
    row = {
        "dos_id": "987654",
        "current_entity_name": "TEST CORP INC",
        "dos_process_zip": "10001",
    }
    record = parse_nys_record(row)
    assert record is not None
    assert record.initial_dos_filing_date is None


def test_nys_model_optional_zip_is_none():
    """
    dos_process_zip is optional — missing it should still
    produce a valid record with None for zip_code.
    """
    row = {
        "dos_id": "987654",
        "current_entity_name": "TEST CORP INC",
        "initial_dos_filing_date": "2015-05-20",
    }
    record = parse_nys_record(row)
    assert record is not None
    assert record.zip_code is None


def test_nys_date_parsing_mm_dd_yyyy():
    """
    MM/DD/YYYY date format should also parse correctly.
    """
    row = {
        "dos_id": "987654",
        "current_entity_name": "TEST CORP INC",
        "initial_dos_filing_date": "05/20/2015",
        "dos_process_zip": "10001",
    }
    record = parse_nys_record(row)
    assert record is not None
    assert record.initial_dos_filing_date == date(2015, 5, 20)


def test_nys_date_parsing_bad_value():
    """
    An unparseable date should become None instead of crashing.
    """
    row = {
        "dos_id": "987654",
        "current_entity_name": "TEST CORP INC",
        "initial_dos_filing_date": "INVALID-DATE",
        "dos_process_zip": "10001",
    }
    record = parse_nys_record(row)
    assert record is not None
    assert record.initial_dos_filing_date is None


# -------------------------------------------------------
# SECTION 4: Parse Helper Tests
# -------------------------------------------------------

def test_parse_nyc_handles_non_dict():
    """
    parse_nyc_record() should return None gracefully
    when passed something that isn't a dict.
    """
    assert parse_nyc_record(["not", "a", "dict"]) is None
    assert parse_nyc_record(None) is None
    assert parse_nyc_record("a string") is None


def test_parse_nys_handles_non_dict():
    """
    parse_nys_record() should return None gracefully
    when passed something that isn't a dict.
    """
    assert parse_nys_record(["not", "a", "dict"]) is None
    assert parse_nys_record(None) is None
    assert parse_nys_record(42) is None


def test_parse_nyc_returns_correct_type(valid_nyc_row):
    """parse_nyc_record() should return a NycDcaBusiness instance."""
    record = parse_nyc_record(valid_nyc_row)
    assert isinstance(record, NycDcaBusiness)


def test_parse_nys_returns_correct_type(valid_nys_row):
    """parse_nys_record() should return a NysCorpEntity instance."""
    record = parse_nys_record(valid_nys_row)
    assert isinstance(record, NysCorpEntity)