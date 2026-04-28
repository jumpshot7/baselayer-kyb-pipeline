-- database.sql
-- KYB Compliance Anomaly Detection Pipeline
-- Three tables:
--  1. nyc_dca_businesses   - NYC DCA Licensed businesses
--  2. nys_corp_entities    - NY State registered corporations (NYC boroughs only)
--  3. kyb_anomalies        - fuzzy match results + anomaly flags

-- Design principles:
-- Indexes only on columns that are actually queried, joined, or ordered by.
-- Unique constraints to prevent duplicate ingestion on pipeline reruns.
-- created_at timestamp for pipeline auditing.

-- Index strategy:
-- We deliberately keep indexes minimal. Each index speeds up reads but
-- slows down INSERT/UPDATE and takes up storage. For a pipeline that
-- bulk-inserts hundreds of thousands of rows, fewer indexes = faster loads.
-- Only 7 indexes total, each justified below.


-- ============================================================
-- Table 1: NYC DCA Businesses
-- ============================================================

CREATE TABLE IF NOT EXISTS nyc_dca_businesses(
    id SERIAL PRIMARY KEY,

    -- Core identity
    license_number      TEXT NOT NULL,
    business_name       TEXT NOT NULL,
    business_unique_id  TEXT,

    -- License details
    business_category   TEXT,
    license_type        TEXT,

    -- Status — used in flag_dormant computation
    -- e.g. "Active", "Expired", "Surrendered", "Inactive"
    license_status      TEXT,

    -- Dates
    initial_issuance_date   DATE,
    expiration_date         DATE,

    -- Contact
    contact_phone       TEXT,

    -- Address
    building_number     TEXT,
    street              TEXT,
    city                TEXT,
    state               TEXT,

    -- Zip — used in fuzzy match zip grouping
    zip_code            TEXT,
    borough             TEXT,

    -- Geo
    latitude            NUMERIC(9, 6),
    longitude           NUMERIC(9, 6),

    -- Pipeline metadata
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Prevents duplicate license rows if pipeline reruns
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_nyc_license_number') THEN
        ALTER TABLE nyc_dca_businesses ADD CONSTRAINT uq_nyc_license_number UNIQUE (license_number);
    END IF;
END $$;

-- Index 1: zip_code
-- Critical — fuzzy matching groups NYC businesses by zip to avoid
-- comparing every business against every NYS entity (N x M explosion).
CREATE INDEX IF NOT EXISTS idx_nyc_zip_code
    ON nyc_dca_businesses (zip_code);

-- Index 2: license_status
-- Used in flag_dormant: WHERE license_status IN ('Expired', 'Surrendered')
-- Also used by the API /anomalies endpoint filters.
CREATE INDEX IF NOT EXISTS idx_nyc_license_status
    ON nyc_dca_businesses (license_status);

-- Index 3: business_name
-- Used by the API /businesses/search endpoint (ILIKE query).
CREATE INDEX IF NOT EXISTS idx_nyc_business_name
    ON nyc_dca_businesses (business_name);

-- Dropped indexes (not worth the cost):
--   idx_nyc_expiration_date  — not used in any flag or API query
--   idx_nyc_borough          — low-traffic filter, not worth slowing inserts


-- ============================================================
-- Table 2: NY State Corporations & Entities
-- ============================================================
-- Stripped to only the 4 columns used in the pipeline.
-- All other NYS fields (entity_type, jurisdiction, dos_process_name,
-- CEO fields, registered agent fields, location fields) are excluded —
-- they are not used in any anomaly flag and would waste storage.

CREATE TABLE IF NOT EXISTS nys_corp_entities(
    id SERIAL PRIMARY KEY,

    -- Unique state-assigned ID — deduplication key
    dos_id                  TEXT NOT NULL,

    -- Name used for fuzzy matching against NYC DCA business names
    current_entity_name     TEXT NOT NULL,

    -- Date the corporation was formed with NYS DOS.
    -- flag_predates: was the NYC license issued before this date?
    -- flag_dormant:  has the entity been around > 3 years with a dead license?
    initial_dos_filing_date DATE,

    -- Zip code of DOS process address.
    -- Used to group NYS entities by zip so fuzzy matching only
    -- compares businesses in the same zip code.
    zip_code                TEXT,

    -- Pipeline metadata
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Prevents duplicate entity rows if pipeline reruns
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_nys_dos_id') THEN
        ALTER TABLE nys_corp_entities ADD CONSTRAINT uq_nys_dos_id UNIQUE (dos_id);
    END IF;
END $$;

-- Index 4: zip_code
-- Critical — fuzzy matching groups NYS entities by zip.
-- Without this, grouping 400K rows by zip on every run would be slow.
CREATE INDEX IF NOT EXISTS idx_nys_zip_code
    ON nys_corp_entities (zip_code);

-- Index 5: initial_dos_filing_date
-- Used in flag_dormant and flag_predates date comparisons.
CREATE INDEX IF NOT EXISTS idx_nys_filing_date
    ON nys_corp_entities (initial_dos_filing_date);

-- Dropped indexes (not worth the cost):
--   idx_nys_entity_name — fuzzy matching uses Python/rapidfuzz in memory,
--                         not SQL LIKE, so Postgres never searches this column


-- ============================================================
-- Table 3: KYB Anomalies
-- ============================================================

CREATE TABLE IF NOT EXISTS kyb_anomalies(
    id SERIAL PRIMARY KEY,

    -- Foreign keys to source tables
    nyc_business_id     INTEGER NOT NULL REFERENCES nyc_dca_businesses(id),
    nys_entity_id       INTEGER NOT NULL REFERENCES nys_corp_entities(id),

    -- Fuzzy match confidence (0.0 - 100.0)
    match_score         NUMERIC(5, 2) NOT NULL,

    -- Flag 1: Active NYC license but NYS entity is dissolved.
    -- Hardcoded FALSE — the NYS Active Corporations dataset only contains
    -- active entities. Needs a dissolved corporations dataset to compute.
    flag_license_active_entity_dissolved    BOOLEAN NOT NULL DEFAULT FALSE,

    -- Flag 2: NYC license issued before the NYS entity was formed.
    -- nyc.initial_issuance_date < nys.initial_dos_filing_date
    flag_license_predates_formation         BOOLEAN NOT NULL DEFAULT FALSE,

    -- Flag 3: License is Expired/Surrendered but entity is still active
    -- in the state registry and was formed more than 3 years ago.
    -- nyc.license_status IN ('Expired','Surrendered')
    -- AND years_since(nys.initial_dos_filing_date) > 3
    flag_entity_dormant                     BOOLEAN NOT NULL DEFAULT FALSE,

    -- Flag 4: NYC license zip != NYS registered address zip.
    -- nyc.zip_code != nys.zip_code (first 5 digits)
    flag_address_mismatch                   BOOLEAN NOT NULL DEFAULT FALSE,

    -- TRUE if ANY flag above is TRUE
    has_anomaly         BOOLEAN NOT NULL DEFAULT FALSE,

    -- Pipeline metadata
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Prevents duplicate anomaly rows if pipeline reruns
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_anomaly_pair') THEN
        ALTER TABLE kyb_anomalies ADD CONSTRAINT uq_anomaly_pair UNIQUE (nyc_business_id, nys_entity_id);
    END IF;
END $$;

-- Index 6: has_anomaly
-- Every API query filters on this column first.
CREATE INDEX IF NOT EXISTS idx_anomalies_has_anomaly
    ON kyb_anomalies (has_anomaly);

-- Index 7: match_score
-- API orders results by match_score DESC.
CREATE INDEX IF NOT EXISTS idx_anomalies_match_score
    ON kyb_anomalies (match_score DESC);

-- Index 8: nyc_business_id + nys_entity_id
-- Used in JOINs in every API query that fetches anomaly details.
CREATE INDEX IF NOT EXISTS idx_anomalies_nyc_id
    ON kyb_anomalies (nyc_business_id);

CREATE INDEX IF NOT EXISTS idx_anomalies_nys_id
    ON kyb_anomalies (nys_entity_id);