import pytest
from datetime import date
from models import (
    NycDcaBusiness,
    NysCorpEntity,
    parse_nyc_record,
    parse_nys_record
)

# Fixtures
# Provide sample "raw" dictionaries simulating row data from the CSVs.

@pytest.fixture
def valid_nyc_row():
    """ Provides a valid dictionary representing a row from the NYC CSV."""
    return {
        "License Number": "123456",
        "Business Name": "TEST BUSINESS LLC",
        "Business Category": "Retail",
        "Initial Issuance Date": "01/15/2020",
        "Latitude": "40.7218",
        "Longitude": "-74.0060"}

@pytest.fixture
def valid_nys_row():
    """Provides a valid dictionary representing a row from the NYS CSV."""
    return{
        "DOS ID": "987654",
        "Current Entity Name": "TEST CORP INC",
        "Entity Type": "DOMESTIC BUSINESS CORPORATION",
        "Date of Formation": "2015-05-20"
    }

# --- NYC DCA Business Tests ---

def test_nyc_model_valid(valid_nyc_row):
    """Test that a vlaid NYC row is parsed correctly, including dates and floats"""
    record = NycDcaBusiness.model_validate(valid_nyc_row)

    assert record.license_number == "123456"
    assert record.business_name == "TEST BUSINESS LLC"
    # Verify the stirng date became a real Python date object
    assert record.initial_issuance_date == date(2020, 1, 15)
    # Verify the stirng coordinates became real Python floats
    assert record.latitude == 40.7218
    assert record.longitude == -74.0060
    
def test_nyc_model_missing_required():
    """Test that the NYC model fails if a required field like License Number is missing."""
    invalid_row = {
        "Business Name": "TEST BUSINESS LLC" # Missing License number
    }

    # The helper function should catch the Pydantic Validation
    result = parse_nyc_record(invalid_row)
    assert result is None

def test_nyc_float_parsing():
    """Test that the lat/long float parser handles bad values gracefully."""
    row = {
        "License Number": "111",
        "Business Name": "FLOAT TEST",
        "Latitude": "BAD_DATA", # Should become None instead of crashing
        "Longitude": "" # Ditto ^^
    }

    record = parse_nyc_record(row)
    assert record is not None
    assert record.latitude is None
    assert record.longitude is None

# --- NYS Corp Entity Tests ---
def test_nys_model_valid(valid_nys_row):
    """Test that a valid NYS row is parsed correctly into the model."""
    record = NysCorpEntity.model_validate(valid_nys_row)
    
    assert record.dos_id == "987654"
    assert record.current_entity_name == "TEST CORP INC"
    # Verify Date of Formation parsed successfully from a different string format
    assert record.date_of_formation == date(2015, 5, 20)

def test_nys_model_missing_required():
    """Test that the NYS model fails if required fields are missing."""
    invalid_row = {
        "DOS ID": "987654" # Missing Current Entity Name
    }
    
    result = parse_nys_record(invalid_row)
    assert result is None

# --- General Parsing Helper Tests ---
def test_parse_helpers_catch_exceptions():
    """Test that the parse helpers gracefully handle completely malformed data types."""
    # simulating a scenario where the pipeline passes a list instead of a dictionary
    bad_data = ["not", "a", "dict"]
    assert parse_nyc_record(bad_data) is None
    assert parse_nys_record(bad_data) is None