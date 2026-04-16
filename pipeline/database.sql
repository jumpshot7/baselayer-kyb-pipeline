-- database.sql
-- KYB Compliance Anomaly Detection Pipeline
-- Three tables:
--  1. nyc_dca_businesses   - NYC DCA Licensed businesses
--  2. nys_corp_entities    - NY State registered corportations
--  3. kyb_anomalies        - fuzzy match results + anomaly flags

-- Design principles:
-- Indexes on every column used in WHERE, JOIN, or ORDER BY
-- Unique constraints to prevent duplicate ingestion
-- created_at timestap for pipeline auditing

-- Table 1: NYC DCA Businesses
-- ============================
-- Store every record from the NYC DVA Legally Operating Businesses dataset. license_number is the natural primary key.
-- Every DCA has a unique number.

CREATE TABLE IF NOT EXISTS nyc_dca_businesses(
    id SERIAL PRIMARY KEY,

    -- Core identity
    license_number TEXT NOT NULL,
    business_name TEXT NOT NULL,
    business_unique_id TEXT,

    -- License details
    business_category TEXT,
    license_type TEXT,
    license_status TEXT,

    -- Dates
    initial_issuance_date DATE,
    expiration_date DATE,

    -- Contact
    contact_phone         TEXT,

    -- Address
    building_number       TEXT,
    street                TEXT,
    city                  TEXT,
    state                 TEXT,
    zip_code              TEXT,
    borough               TEXT,

    -- Geo
    latitude              NUMERIC(9, 6),
    longitude             NUMERIC(9, 6),

    -- Pipeline metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Unique Constraints: prevent the same license from being inserted twice if the pipeline runs more than once
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_nyc_license_number') THEN
        ALTER TABLE nyc_dca_businesses ADD CONSTRAINT uq_nyc_license_number UNIQUE (license_number);
    END IF;
END $$;

-- Indexes for the columns we'll query and join on most often
CREATE INDEX IF NOT EXISTS idx_nyc_business_name
    ON nyc_dca_businesses (business_name);

CREATE INDEX IF NOT EXISTS idx_nyc_license_status
    ON nyc_dca_businesses (license_status);

CREATE INDEX IF NOT EXISTS idx_nyc_expiration_date
    ON nyc_dca_businesses (expiration_date);

CREATE INDEX IF NOT EXISTS idx_nyc_borough
    ON nyc_dca_businesses (borough);

CREATE INDEX IF NOT EXISTS idx_nyc_zip_code
    ON nyc_dca_businesses (zip_code);

-- Table 2: NY State Corportations & Entities
-- Stores every record from the NY State DOS corporation registry.
-- dos__ids is the natural primary key - the state assigns a unique ID to every registered entity

CREATE TABLE IF NOT EXISTS nys_corp_entities(
    id SERIAL PRIMARY KEY,

    -- Core identity
    dos_id TEXT NOT NULL,
    current_entity_name TEXT NOT NULL,
    entity_type TEXT,

    -- Status — key field for anomaly detection
    -- e.g. "Active", "Inactive", "Dissolved"
    dos_process_name      TEXT,
    county                TEXT,
    jurisdiction          TEXT,

    -- Dates
    date_of_formation     DATE,

    -- Address
    street_address        TEXT,
    city                  TEXT,
    state                 TEXT,
    zip_code              TEXT,

    -- Pipeline metadata
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Unique constraint: one row per DOS entity ID
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_nys_dos_id') THEN
        ALTER TABLE nys_corp_entities ADD CONSTRAINT uq_nys_dos_id UNIQUE (dos_id);
    END IF;
END $$;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_nys_entity_name
    ON nys_corp_entities (current_entity_name);

CREATE INDEX IF NOT EXISTS idx_nys_dos_process_name
    ON nys_corp_entities (dos_process_name);

CREATE INDEX IF NOT EXISTS idx_nys_zip_code
    ON nys_corp_entities (zip_code);

-- TABLE 3: KYB Anomalies
-- Most important table, stores the results of the fuzzy matching algorithm and the anomaly flags.
-- Each row represents one potential match between a NYC DCA business and a NY State Corporation, along with a score
-- indicating how confident the match is, and flags for each type of anomaly detected.

CREATE TABLE IF NOT EXISTS kyb_anomalies(
    id SERIAL PRIMARY KEY,

    -- Foreign keys linking to the two source tables
    nyc_business_id INTEGER NOT NULL REFERENCES nyc_dca_businesses(id),
    nys_entity_id INTEGER NOT NULL REFERENCES nys_corp_entities(id),

    -- Fuzzy match confidence score (0.0 - 100.0)
    -- rapidfuzz returns a score out of 100
    match_score NUMERIC(5, 2) NOT NULL,

    -- NYC license is active but NY State entity is dissolved
    flag_license_active_entity_dissolved BOOLEAN NOT NULL DEFAULT FALSE,

    -- NYC license was issued before the entity was even formed
    flag_license_predates_formation BOOLEAN NOT NULL DEFAULT FALSE,

    -- NYC license is active but entity hasn't filed in 3+ years
    flag_entity_dormant BOOLEAN NOT NULL DEFAULT FALSE,

    -- Adddress on license doesn't match registered address
    flag_address_mismatch BOOLEAN NOT NULL DEFAULT FALSE,

    -- Convenience column: true if ANY flag is true. Makes it easy to query all anomalies with one filter
    has_anomaly BOOLEAN NOT NULL DEFAULT FALSE,

    -- Pipeline metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Unique constraint: one match result per NYC+NYS pair
-- Prevents duplicate anomaly rows if pipeline reruns
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_anomaly_pair') THEN
        ALTER TABLE kyb_anomalies ADD CONSTRAINT uq_anomaly_pair UNIQUE (nyc_business_id, nys_entity_id);
    END IF;
END $$;

-- Indexes for the API queries in api.py
CREATE INDEX IF NOT EXISTS idx_anomalies_has_anomaly
    ON kyb_anomalies (has_anomaly);

CREATE INDEX IF NOT EXISTS idx_anomalies_match_score
    ON kyb_anomalies (match_score DESC);

CREATE INDEX IF NOT EXISTS idx_anomalies_nyc_id
    ON kyb_anomalies (nyc_business_id);

CREATE INDEX IF NOT EXISTS idx_anomalies_nys_id
    ON kyb_anomalies (nys_entity_id);

CREATE INDEX IF NOT EXISTS idx_anomalies_flag_dissolved
    ON kyb_anomalies (flag_license_active_entity_dissolved)
    WHERE flag_license_active_entity_dissolved = TRUE;

CREATE INDEX IF NOT EXISTS idx_anomalies_flag_predates
    ON kyb_anomalies (flag_license_predates_formation)
    WHERE flag_license_predates_formation = TRUE;