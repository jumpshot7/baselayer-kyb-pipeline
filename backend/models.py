"""
models.py

Pydantic models for both datasets:
1. NycDcaBusiness - NYC DCA Legally Operating Businesses
2. NysCorpEntity  - NY State Active Corporations (NYC boroughs only)

These models do three things:
- Define the shape of each record (field names + types)
- Validate every record as it comes out of the CSV
- Coerce messy strings into proper Python types (dates, floats, etc)

Apache Beam will read each CSV row as a raw dict, pass it into these
models, and only forward records that pass validation.
"""

import logging
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, ConfigDict

logger = logging.getLogger(__name__)


# ============================================================
# Section 1: NYC DCA Business Model
# ============================================================
# Each field maps directly to a column in the NYC DCA CSV.
# Aliases match the original CSV header names (title case with spaces).
# Optional fields handle the reality that government data is messy
# and not every field is populated on every record.

class NycDcaBusiness(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # Core identity
    license_number:     str            = Field(..., alias="License Number")
    business_name:      str            = Field(..., alias="Business Name")
    business_unique_id: Optional[str]  = Field(None, alias="Business Unique ID")
    business_category:  Optional[str]  = Field(None, alias="Business Category")
    license_type:       Optional[str]  = Field(None, alias="License Type")

    # License status — used in flag_dormant
    # e.g. "Active", "Expired", "Surrendered", "Inactive"
    license_status:         Optional[str]  = Field(None, alias="License Status")
    initial_issuance_date:  Optional[date] = Field(None, alias="Initial Issuance Date")
    expiration_date:        Optional[date] = Field(None, alias="Expiration Date")

    # Contact + address
    contact_phone:  Optional[str]   = Field(None, alias="Contact Phone")
    building_number: Optional[str]  = Field(None, alias="Building Number")
    street:         Optional[str]   = Field(None, alias="Street1")
    city:           Optional[str]   = Field(None, alias="City")
    state:          Optional[str]   = Field(None, alias="State")
    zip_code:       Optional[str]   = Field(None, alias="ZIP Code")
    borough:        Optional[str]   = Field(None, alias="Borough")

    # Geo
    latitude:  Optional[float] = Field(None, alias="Latitude")
    longitude: Optional[float] = Field(None, alias="Longitude")

    @field_validator("initial_issuance_date", "expiration_date", mode="before")
    @classmethod
    def parse_dates(cls, v):
        if not v or v == "":
            return None
        for fmt in ("%m/%d/%Y", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d"):
            try:
                return datetime.strptime(v, fmt).date()
            except ValueError:
                continue
        logger.warning(f"Could not parse date: {v}")
        return None

    @field_validator("latitude", "longitude", mode="before")
    @classmethod
    def parse_floats(cls, v):
        if not v or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


# ============================================================
# Section 2: NY State Corporation Model
# ============================================================
# Stripped to only the 4 columns we actually use in the pipeline.
# All other NYS fields (entity_type, jurisdiction, dos_process_name,
# CEO address, registered agent address, location address) have been
# deliberately excluded — they are not used in any anomaly flag
# computation and would waste storage on the free database tier.
#
# Column aliases are snake_case because data comes from the Socrata
# /resource/ API endpoint which returns snake_case field names,
# not the original CSV title-case headers.
#
# Fields and their purpose:
#   dos_id                  -> unique key, prevents duplicate rows
#   current_entity_name     -> fuzzy name matching vs NYC businesses
#   initial_dos_filing_date -> flag_predates + flag_dormant calculations
#   zip_code                -> zip-based grouping in fuzzy matching

class NysCorpEntity(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    dos_id:                  str            = Field(..., alias="dos_id")
    current_entity_name:     str            = Field(..., alias="current_entity_name")

    # Date the corporation was formed with NYS DOS.
    # Used for:
    #   flag_predates -> nyc.initial_issuance_date < this date?
    #   flag_dormant  -> years_since(this date) > 3 with dead NYC license?
    initial_dos_filing_date: Optional[date] = Field(None, alias="initial_dos_filing_date")

    # Zip code of DOS process address.
    # Used to group NYS entities by zip so fuzzy matching only compares
    # businesses in the same zip code — avoids an N x M explosion.
    zip_code: Optional[str] = Field(None, alias="dos_process_zip")

    @field_validator("initial_dos_filing_date", mode="before")
    @classmethod
    def parse_date(cls, v):
        if not v or v == "":
            return None
        for fmt in ("%m/%d/%Y", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d"):
            try:
                return datetime.strptime(v, fmt).date()
            except ValueError:
                continue
        logger.warning(f"Could not parse NYS date: {v}")
        return None


# ============================================================
# Section 3: Parsing Helpers
# ============================================================
# These are called by both the Beam DoFns (production) and the
# local Python loader (development). Each takes a raw CSV row dict
# and returns a validated Pydantic model, or None if validation fails.
# Beam filters out the Nones downstream via the DoFn yield.

def parse_nyc_record(raw: dict) -> Optional[NycDcaBusiness]:
    """Parse a raw CSV row dict into a NycDcaBusiness. Returns None if invalid."""
    try:
        return NycDcaBusiness.model_validate(raw)
    except Exception as e:
        logger.warning(f"NYC record failed validation: {e} | raw={raw}")
        return None


def parse_nys_record(raw: dict) -> Optional[NysCorpEntity]:
    """Parse a raw CSV row dict into a NysCorpEntity. Returns None if invalid."""
    try:
        return NysCorpEntity.model_validate(raw)
    except Exception as e:
        logger.warning(f"NYS record failed validation: {e} | raw={raw}")
        return None