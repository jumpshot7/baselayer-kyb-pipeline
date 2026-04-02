"""
models.py

Pydantic models for both datasets:
1. NycDcaBusiness - NYC DCA Legally Operating Businesses
2. NysCorpEntity - NY State Corporations & Entities

These models do three things:
- Define the shape of each record (field names + types)
- Validate every record as it comes out of the CSV
- Coerce messy strings into propert Python types (dates, floats, etc)

Apache Beam will read each CSV row as a raw dict, pass it into these models, and only forward records that pass validation
"""

import logging
from datetime import date
from typing import Optional

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


# Section 1: NYC DCA Business Model
# Each field maps directly to a column in the CSV
# Optional[str] means the field can be missing or None, government data is inherently messy and not every field is always populated

class NycDcaBusiness(BaseModel):
    # Core identity fields
    license_number: str = Field(..., alias="License Number")
    business_name: str = Field(..., alias="Business Name")
    dba_trade_name: Optional[str] = Field(None, alias="DBA/Trade Name")
    business_unique_id: Optional[str] = Field(None, alias="Business Unique ID")

    # License details
    business_category: Optional[str] = Field(None, alias="Business Category")
    license_type: Optional[str] = Field(None, alias="License Type")
    license_status: Optional[str] = Field(None, alias="License Status")

    # Dates - stored as Python date object, not raw strings
    initial_issuance_date: Optional[date] = Field(None, alias="Initial Issunce Date")
    expiration_date: Optional[date] = Field(None, alias="Expiration Date")

    # Contact
    contact_phone: Optional[str] = Field(None, alias="Contact Phone")

    # Address
    building_number: Optional[str] = Field(None, alias="Building Number")
    street: Optional[str] = Field(None, alias="Street1")
    city:                 Optional[str]  = Field(None, alias="City")
    state:                Optional[str]  = Field(None, alias="State")
    zip_code:             Optional[str]  = Field(None, alias="ZIP Code")
    borough:              Optional[str]  = Field(None, alias="Borough")

    # Geo
    latitude:             Optional[float] = Field(None, alias="Latitude")
    longitude:            Optional[float] = Field(None, alias="Longitude")

    # Validators
    # Socrata returns dates as a string like "06/19/2026"
    # We need to parse them into Python date objects
    # @field_validator runs automatically on the field value
    # before it gets stored on the model.
    @field_validator("initial_issuance_date", "expiration_date", mode="before")
    @classmethod
    def parse_dates(cls, v):
        if not v or v == "":
            return None
        for fmt in ("%m/%d/%Y", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d"):
            try:
                from datetime import datetime
                return datetime.strptime(v, fmt).date()
            except ValueError:
                continue
        logger.warning(f"Could not parse date: {v}")
        return None
    
    # Latitude and longitude come as strings from the CSV
    # We will try to cast them to float, and return None if they are empty or unparseable.
    @field_validator("latitude", "longitude", mode="before")
    @classmethod
    def parse_floats(cls, v):
        if not v or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
    
    class Config:
        # Allow field aliases so Pydantic maps "License Number" -> license_number automatically
        populate_by_name = True
    
    # Section 2: NY State Corporation Model
    # Core identity
    dos_id:               str            = Field(..., alias="DOS ID")
    current_entity_name:  str            = Field(..., alias="Current Entity Name")
    entity_type:          Optional[str]  = Field(None, alias="Entity Type")

    # Status — this is the key field for anomaly detection
    # e.g. "Active", "Inactive", "Dissolved"
    dos_process_name:     Optional[str]  = Field(None, alias="DOS Process Name")
    county:               Optional[str]  = Field(None, alias="County")
    jurisdiction:         Optional[str]  = Field(None, alias="Jurisdiction")

    # Dates
    date_of_formation:    Optional[date] = Field(None, alias="Date of Formation")
    date_of_dissolution:  Optional[date] = Field(None, alias="Date of Dissolution")

    # Address
    street_address:       Optional[str]  = Field(None, alias="Street Address")
    city:                 Optional[str]  = Field(None, alias="City")
    state:                Optional[str]  = Field(None, alias="State")
    zip_code:             Optional[str]  = Field(None, alias="Zip Code")

    @field_validator("date_of_formation", "date_of_dissoultion", mode="before")
    @classmethod
    def parse_date(cls, v):
        if not v or v == "":
            return None
        for fmt in ("%m/%d/%Y", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d"):
            try:
                from datetime import datetime
                return datetime.strptime(v, fmt).date()
            except ValueError:
                continue
        logger.warning(f"Could not parse date: {v}")
        return None

        class Config:
            populate_by_name = True
    
    # Section 3: Parsing Helpers
    # These functions are what Apache Beam will call. Each one takes a raw dict (one CSV row) and tries to parse it into
    # the Pydantic model. If validation fails, it logs the error and returns None. Beam will filter out the Nones downstream.

    def parse_nyc_records(raw: dict) -> Optional[NycDcaBusiness]:
        """
        Parse a raw CSV row dict into a NycDcaBusiness
        Return None if validation fails.
        """
        try:
            return NycDcaBusiness.model_validate(raw)
        except Exception as e:
            logger.warning(f"NYC record failed validation: {e} | raw={raw}")
            return None
    
    def parse_nys_records(raw: dict) -> Optional[NysCorpEntity]:
        """
        Parse a raw CSV row dict into a NysCorpEntity.
        Returns None if validation fails.
        """
        try:
            return NysCorpEntity.model_validate(raw)
        except Exception as e:
            logger.warning(f"NYS record failed validation: {e} | raw={raw}")
            return None



