"""
execution.py

Orchestrates the full KYB pipeline:
1. Create DB tables if they don't exist
2. Check if DB is already populated (skip if so)
3. Fetch raw CSVs from Socrata API -> upload to GCS (fetcher.py)
4. Load CSVs into Postgres (Beam for production, plain Python for local dev)
5. Fuzzy match NYC businesses against NYS entities
6. Compute anomaly flags
7. Write results to kyb_anomalies

--- ANOMALY FLAGS ---

flag_license_active_entity_dissolved:
    Hardcoded FALSE. The NYS "Active Corporations" dataset only contains
    active entities by definition. A separate dissolved corporations dataset
    would be needed to compute this flag.

flag_license_predates_formation:
    TRUE when the NYC DCA license was issued before the NYS entity was
    even formed. Signal: how was a license granted for a non-existent entity?
    Computed from: nyc.initial_issuance_date < nys.initial_dos_filing_date

flag_entity_dormant:
    TRUE when the NYC license is Expired or Surrendered AND the NYS entity
    has been active for more than 3 years. Signal: business stopped operating
    but never formally dissolved its legal entity with the state.
    Computed from: nyc.license_status IN ('Expired', 'Surrendered')
                   AND years_since(nys.initial_dos_filing_date) > 3

flag_address_mismatch:
    TRUE when the zip code on the NYC license doesn't match the zip code
    of the NYS registered address. Signal: entity may be operating from
    an unregistered location.
    Computed from: nyc.zip_code != nys.zip_code

--- RUNNER MODES ---

Production (Google Cloud Dataflow):
    USE_LOCAL_RUNNER=false (or not set)
    -> Apache Beam pipeline designed for Google Cloud Dataflow
    -> GCP spins up multiple workers, processes in parallel, shuts down
    -> Handles tens of millions of rows with no memory or timeout issues

Local development (Codespace / Docker):
    USE_LOCAL_RUNNER=true in .env
    -> Plain Python CSV streaming + psycopg2 batch inserts
    -> No gRPC, no workers, no timeouts
    -> Beam's DirectRunner hits gRPC deadline timeouts on large datasets
       in single-container environments. This is the standard pattern:
       Dataflow for prod, local fallback for dev.
"""

import csv
import io
import logging
import os
import time
import psycopg2
import psycopg2.extras
import apache_beam as beam

from datetime import date, datetime
from typing import Optional
from dotenv import load_dotenv
from rapidfuzz import fuzz
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from google.cloud import storage
from fetcher import run as fetch_data

from models import (
    NycDcaBusiness,
    NysCorpEntity,
    parse_nyc_record,
    parse_nys_record,
)

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USE_LOCAL_RUNNER = os.getenv("USE_LOCAL_RUNNER", "false").lower() == "true"

MAX_RETRIES = 3
RETRY_DELAY = 5
BATCH_SIZE = 500
FLUSH_EVERY = 200

MATCH_THRESHOLD = 85.0

# License statuses that indicate the business is no longer operating
DEAD_LICENSE_STATUSES = {"Expired", "Surrendered", "Inactive"}

# How many years since formation before we consider an entity dormant
DORMANT_YEARS_THRESHOLD = 3


# ============================================================
# 1. Database Connection
# ============================================================

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# ============================================================
# 2. Table Setup
# ============================================================

def create_tables():
    sql_path = os.path.join(os.path.dirname(__file__), "../pipeline/database.sql")
    with open(sql_path, "r") as f:
        sql = f.read()

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("Tables created successfully")
    finally:
        conn.close()


# ============================================================
# 3. Idempotency Check
# ============================================================

def tables_are_populated() -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM nyc_dca_businesses")
            nyc_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM nys_corp_entities")
            nys_count = cur.fetchone()[0]

        logger.info(f"NYC rows: {nyc_count} | NYS rows: {nys_count}")
        return nyc_count > 0 and nys_count > 0
    finally:
        conn.close()


# ============================================================
# 4a. Apache Beam Pipelines (Production / Google Cloud Dataflow)
#
# This is the real implementation. On Dataflow, GCP automatically
# spins up multiple workers, distributes the CSV across them,
# processes rows in parallel, and shuts workers down when done.
#
# To run on Dataflow:
#   - Set USE_LOCAL_RUNNER=false (or remove it)
#   - Set GCP_PROJECT in .env
#   - Ensure GOOGLE_APPLICATION_CREDENTIALS is set
# ============================================================

class ParseNycCsvLine(beam.DoFn):
    """Parses a raw CSV string line into a dict. Skips malformed rows."""
    def __init__(self, headers):
        self.headers = headers

    def process(self, element: str):
        row = next(csv.reader([element]))
        if len(row) != len(self.headers):
            logger.warning(f"Skipping malformed NYC line: {element[:80]}")
            return
        yield dict(zip(self.headers, row))


class ParseNysCsvLine(beam.DoFn):
    """Same as ParseNycCsvLine but for the NYS dataset."""
    def __init__(self, headers):
        self.headers = headers

    def process(self, element: str):
        row = next(csv.reader([element]))
        if len(row) != len(self.headers):
            logger.warning(f"Skipping malformed NYS line: {element[:80]}")
            return
        yield dict(zip(self.headers, row))


class ParseAndValidateNyc(beam.DoFn):
    """Validates each dict into a NycDcaBusiness Pydantic model. Drops invalid rows."""
    def process(self, element):
        result = parse_nyc_record(element)
        if result is not None:
            yield result


class ParseAndValidateNys(beam.DoFn):
    """Validates each dict into a NysCorpEntity Pydantic model. Drops invalid rows."""
    def process(self, element):
        result = parse_nys_record(element)
        if result is not None:
            yield result


class WriteNycToPostgres(beam.DoFn):
    """
    Bulk inserts validated NycDcaBusiness models into Postgres.
    Flushes every FLUSH_EVERY rows mid-bundle to keep memory flat.
    """

    def _connect(self):
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT", 5432),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    def setup(self):
        self._connect()

    def teardown(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def start_bundle(self):
        self.batch = []

    def _flush(self):
        if not self.batch:
            return
        sql = """
            INSERT INTO nyc_dca_businesses (
                license_number, business_name,
                business_unique_id, business_category, license_type,
                license_status, initial_issuance_date, expiration_date,
                contact_phone, building_number, street, city, state,
                zip_code, borough, latitude, longitude
            ) VALUES %s
            ON CONFLICT (license_number) DO NOTHING
        """
        for attempt in range(MAX_RETRIES):
            try:
                if self.conn.closed:
                    self._connect()
                with self.conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, sql, self.batch, page_size=200)
                self.conn.commit()
                logger.info(f"NYC Beam: flushed {len(self.batch)} rows")
                self.batch = []
                return
            except Exception as e:
                logger.warning(f"NYC Beam write failed (attempt {attempt + 1}): {e}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    self._connect()
                else:
                    raise

    def process(self, element: NycDcaBusiness):
        self.batch.append((
            element.license_number, element.business_name,
            element.business_unique_id, element.business_category,
            element.license_type, element.license_status,
            element.initial_issuance_date, element.expiration_date,
            element.contact_phone, element.building_number,
            element.street, element.city, element.state,
            element.zip_code, element.borough,
            element.latitude, element.longitude,
        ))
        if len(self.batch) >= FLUSH_EVERY:
            self._flush()

    def finish_bundle(self):
        self._flush()


class WriteNysToPostgres(beam.DoFn):
    """
    Bulk inserts validated NysCorpEntity models into Postgres.
    Only writes the 4 columns we actually use — dos_id, current_entity_name,
    initial_dos_filing_date, zip_code.
    """

    def _connect(self):
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT", 5432),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    def setup(self):
        self._connect()

    def teardown(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def start_bundle(self):
        self.batch = []

    def _flush(self):
        if not self.batch:
            return
        sql = """
            INSERT INTO nys_corp_entities (
                dos_id, current_entity_name,
                initial_dos_filing_date, zip_code
            ) VALUES %s
            ON CONFLICT (dos_id) DO NOTHING
        """
        for attempt in range(MAX_RETRIES):
            try:
                if self.conn.closed:
                    self._connect()
                with self.conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, sql, self.batch, page_size=200)
                self.conn.commit()
                logger.info(f"NYS Beam: flushed {len(self.batch)} rows")
                self.batch = []
                return
            except Exception as e:
                logger.warning(f"NYS Beam write failed (attempt {attempt + 1}): {e}")
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    self._connect()
                else:
                    raise

    def process(self, element: NysCorpEntity):
        self.batch.append((
            element.dos_id,
            element.current_entity_name,
            element.initial_dos_filing_date,
            element.zip_code,
        ))
        if len(self.batch) >= FLUSH_EVERY:
            self._flush()

    def finish_bundle(self):
        self._flush()


def get_csv_headers(bucket_name, blob_name):
    """Downloads first 2000 bytes of a GCS CSV to extract headers."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    first_bytes = blob.download_as_bytes(start=0, end=2000)
    first_line = first_bytes.decode("utf-8").split("\n")[0]
    return next(csv.reader([first_line]))


def run_beam_pipelines():
    """
    Runs NYC and NYS Beam pipelines sequentially.
    Designed for Google Cloud Dataflow in production.
    """
    bucket_name = os.getenv("GCS_BUCKET_NAME")

    options = PipelineOptions([
        "--runner=DataflowRunner",
        f"--project={os.getenv('GCP_PROJECT')}",
        "--region=us-east1",
        f"--temp_location=gs://{bucket_name}/tmp",
        f"--staging_location=gs://{bucket_name}/staging",
    ])

    nyc_headers = get_csv_headers(bucket_name, "raw/nyc-dca-businesses.csv")
    nys_headers = get_csv_headers(bucket_name, "raw/nys-corporations.csv")

    logger.info("Running Beam pipeline for NYC DCA businesses (Dataflow)...")
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadNycCsv" >> ReadFromText(
                f"gs://{bucket_name}/raw/nyc-dca-businesses.csv",
                skip_header_lines=1
            )
            | "ParseNycCsv" >> beam.ParDo(ParseNycCsvLine(nyc_headers))
            | "ValidateNyc" >> beam.ParDo(ParseAndValidateNyc())
            | "WriteNycToDB" >> beam.ParDo(WriteNycToPostgres())
        )
    logger.info("NYC Beam pipeline complete")

    logger.info("Running Beam pipeline for NYS corporations (Dataflow)...")
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadNysCsv" >> ReadFromText(
                f"gs://{bucket_name}/raw/nys-corporations.csv",
                skip_header_lines=1
            )
            | "ParseNysCsv" >> beam.ParDo(ParseNysCsvLine(nys_headers))
            | "ValidateNys" >> beam.ParDo(ParseAndValidateNys())
            | "WriteNysToDB" >> beam.ParDo(WriteNysToPostgres())
        )
    logger.info("NYS Beam pipeline complete")


# ============================================================
# 4b. Local Python Loader (Development / Codespace)
# ============================================================

def _insert_batch(conn, sql: str, batch: list, label: str):
    """Bulk insert a batch of rows with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, batch, page_size=BATCH_SIZE)
            conn.commit()
            return
        except Exception as e:
            logger.warning(f"{label} batch insert failed (attempt {attempt + 1}): {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"{label} batch insert failed after all retries")
                raise


def load_nyc_from_gcs():
    """Streams NYC DCA CSV from GCS, validates, inserts into Postgres in batches."""
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    logger.info("[local] Downloading NYC CSV from GCS...")

    client = storage.Client()
    blob = client.bucket(bucket_name).blob("raw/nyc-dca-businesses.csv")
    text = blob.download_as_bytes().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    sql = """
        INSERT INTO nyc_dca_businesses (
            license_number, business_name,
            business_unique_id, business_category, license_type,
            license_status, initial_issuance_date, expiration_date,
            contact_phone, building_number, street, city, state,
            zip_code, borough, latitude, longitude
        ) VALUES %s
        ON CONFLICT (license_number) DO NOTHING
    """

    conn = get_conn()
    batch = []
    total_written = 0
    total_skipped = 0

    try:
        for raw_row in reader:
            record = parse_nyc_record(raw_row)
            if record is None:
                total_skipped += 1
                continue

            batch.append((
                record.license_number, record.business_name,
                record.business_unique_id, record.business_category,
                record.license_type, record.license_status,
                record.initial_issuance_date, record.expiration_date,
                record.contact_phone, record.building_number,
                record.street, record.city, record.state,
                record.zip_code, record.borough,
                record.latitude, record.longitude,
            ))

            if len(batch) >= BATCH_SIZE:
                _insert_batch(conn, sql, batch, "NYC")
                total_written += len(batch)
                logger.info(f"[local] NYC: {total_written} rows inserted...")
                batch = []

        if batch:
            _insert_batch(conn, sql, batch, "NYC")
            total_written += len(batch)

        logger.info(f"[local] NYC load complete: {total_written} written, {total_skipped} skipped")
    finally:
        conn.close()


def load_nys_from_gcs():
    """
    Streams NYS corporations CSV from GCS, validates, inserts into Postgres.
    Only inserts the 4 columns we use: dos_id, current_entity_name,
    initial_dos_filing_date, zip_code.
    """
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    logger.info("[local] Downloading NYS CSV from GCS...")

    client = storage.Client()
    blob = client.bucket(bucket_name).blob("raw/nys-corporations.csv")
    text = blob.download_as_bytes().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    sql = """
        INSERT INTO nys_corp_entities (
            dos_id, current_entity_name,
            initial_dos_filing_date, zip_code
        ) VALUES %s
        ON CONFLICT (dos_id) DO NOTHING
    """

    conn = get_conn()
    batch = []
    total_written = 0
    total_skipped = 0

    try:
        for raw_row in reader:
            record = parse_nys_record(raw_row)
            if record is None:
                total_skipped += 1
                continue

            batch.append((
                record.dos_id,
                record.current_entity_name,
                record.initial_dos_filing_date,
                record.zip_code,
            ))

            if len(batch) >= BATCH_SIZE:
                _insert_batch(conn, sql, batch, "NYS")
                total_written += len(batch)
                logger.info(f"[local] NYS: {total_written} rows inserted...")
                batch = []

        if batch:
            _insert_batch(conn, sql, batch, "NYS")
            total_written += len(batch)

        logger.info(f"[local] NYS load complete: {total_written} written, {total_skipped} skipped")
    finally:
        conn.close()


# ============================================================
# 5. Anomaly Flag Computation
# ============================================================

def years_since(d: date) -> float:
    """Return how many years have passed since a given date."""
    return (date.today() - d).days / 365.25


def compute_anomaly_flags(nyc, nys) -> dict:
    """
    Computes all anomaly flags for a matched NYC/NYS pair.

    nyc and nys are lightweight objects with the fields we need,
    built in run_fuzzy_matching() from the raw DB row tuples.
    """

    # Flag 1: Active NYC license but NYS entity is dissolved.
    # Cannot be computed — the NYS Active Corporations dataset only
    # contains active entities. Hardcoded False until a dissolved
    # corporations dataset is available.
    flag_dissolved = False

    # Flag 2: NYC license was issued before the NYS entity was formed.
    # Real signal: a license cannot legally predate the entity it was
    # issued to. Possible data fraud or clerical error.
    flag_predates = (
        nyc.initial_issuance_date is not None and
        nys.initial_dos_filing_date is not None and
        nyc.initial_issuance_date < nys.initial_dos_filing_date
    )

    # Flag 3: Business stopped operating (Expired/Surrendered license)
    # but never formally dissolved its NYS legal entity.
    # A dormant entity that still exists as a legal shell is a KYB red flag —
    # it could be reactivated or used for fraud without a new license.
    flag_dormant = (
        nyc.license_status is not None and
        nyc.license_status in DEAD_LICENSE_STATUSES and
        nys.initial_dos_filing_date is not None and
        years_since(nys.initial_dos_filing_date) > DORMANT_YEARS_THRESHOLD
    )

    # Flag 4: Zip code on NYC license doesn't match NYS registered address.
    # Signal: entity may be operating from an address it hasn't registered
    # with the state, which is a compliance violation.
    flag_address = (
        nyc.zip_code is not None and
        nys.zip_code is not None and
        nyc.zip_code.strip()[:5] != nys.zip_code.strip()[:5]
    )

    return {
        "flag_license_active_entity_dissolved": flag_dissolved,
        "flag_license_predates_formation": flag_predates,
        "flag_entity_dormant": flag_dormant,
        "flag_address_mismatch": flag_address,
        "has_anomaly": any([flag_dissolved, flag_predates, flag_dormant, flag_address]),
    }


# ============================================================
# 6. Fuzzy Matching
# ============================================================

def run_fuzzy_matching():
    """
    Loads both tables from Postgres, groups NYS entities by zip code,
    then fuzzy matches NYC business names against NYS entity names
    within the same zip code. Computes anomaly flags for each match
    above the score threshold.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Load all NYC businesses with the fields needed for flag computation
            cur.execute("""
                SELECT id, business_name, license_status,
                       initial_issuance_date, zip_code
                FROM nyc_dca_businesses
            """)
            nyc_rows = cur.fetchall()

            # Load all NYS entities with the fields needed for flag computation
            cur.execute("""
                SELECT id, current_entity_name,
                       initial_dos_filing_date, zip_code
                FROM nys_corp_entities
            """)
            nys_rows = cur.fetchall()

        logger.info(f"Loaded {len(nyc_rows)} NYC rows and {len(nys_rows)} NYS rows for matching")

        # Group NYS entities by zip code.
        # Without this, we'd compare every NYC business against every NYS entity
        # which is millions x millions of comparisons — computationally impossible.
        # With zip grouping we only compare pairs that share a zip code.
        nys_by_zip = {}
        for row in nys_rows:
            # Normalize to 5-digit zip to handle ZIP+4 format (e.g. "10001-1234")
            zip_code = (row[3] or "").strip()[:5]
            if zip_code not in nys_by_zip:
                nys_by_zip[zip_code] = []
            nys_by_zip[zip_code].append(row)

        anomalies = []
        matched_pairs = 0

        for nyc_row in nyc_rows:
            nyc_id = nyc_row[0]
            nyc_name = nyc_row[1] or ""
            nyc_zip = (nyc_row[4] or "").strip()[:5]

            candidates = nys_by_zip.get(nyc_zip, [])

            for nys_row in candidates:
                nys_id = nys_row[0]
                nys_name = nys_row[1] or ""

                # token_sort_ratio handles word order differences:
                # "JOES PIZZA LLC" vs "LLC JOES PIZZA" -> score of 100
                score = fuzz.token_sort_ratio(nyc_name.upper(), nys_name.upper())

                if score >= MATCH_THRESHOLD:
                    matched_pairs += 1

                    # Build lightweight objects for flag computation
                    nyc_obj = type("NYC", (), {
                        "license_status": nyc_row[2],
                        "initial_issuance_date": nyc_row[3],
                        "zip_code": nyc_row[4],
                    })()

                    nys_obj = type("NYS", (), {
                        "initial_dos_filing_date": nys_row[2],
                        "zip_code": nys_row[3],
                    })()

                    flags = compute_anomaly_flags(nyc_obj, nys_obj)

                    anomalies.append({
                        "nyc_business_id": nyc_id,
                        "nys_entity_id": nys_id,
                        "match_score": score,
                        **flags,
                    })

        anomaly_count = sum(1 for a in anomalies if a["has_anomaly"])
        logger.info(f"Found {matched_pairs} matches above threshold {MATCH_THRESHOLD}")
        logger.info(f"Of those, {anomaly_count} have at least one anomaly flag")
        return anomalies

    finally:
        conn.close()


# ============================================================
# 7. Write Anomalies to Postgres
# ============================================================

def write_anomalies(anomalies: list[dict]):
    if not anomalies:
        logger.info("No anomalies to write")
        return

    # Only write pairs where at least one flag fired.
    # Pairs with all-FALSE flags are matched names but have no compliance
    # issue — storing them would bloat the table with useless noise.
    anomalies_to_write = [a for a in anomalies if a["has_anomaly"]]
    skipped = len(anomalies) - len(anomalies_to_write)
    logger.info(
        f"Filtered {len(anomalies)} matches -> "
        f"{len(anomalies_to_write)} with anomalies, {skipped} clean pairs skipped"
    )

    if not anomalies_to_write:
        logger.info("No anomalies found after filtering")
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for a in anomalies_to_write:
                cur.execute("""
                    INSERT INTO kyb_anomalies (
                        nyc_business_id, nys_entity_id, match_score,
                        flag_license_active_entity_dissolved,
                        flag_license_predates_formation,
                        flag_entity_dormant,
                        flag_address_mismatch,
                        has_anomaly
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nyc_business_id, nys_entity_id) DO NOTHING
                """, (
                    a["nyc_business_id"], a["nys_entity_id"], a["match_score"],
                    a["flag_license_active_entity_dissolved"],
                    a["flag_license_predates_formation"],
                    a["flag_entity_dormant"],
                    a["flag_address_mismatch"],
                    a["has_anomaly"],
                ))
        conn.commit()
        logger.info(f"Wrote {len(anomalies_to_write)} anomaly records to Postgres")
    finally:
        conn.close()


# ============================================================
# 8. Orchestrator
# ============================================================

def run():
    logger.info("=== execution.py starting ===")
    logger.info(f"Runner mode: {'LOCAL (plain Python)' if USE_LOCAL_RUNNER else 'PRODUCTION (Apache Beam / Dataflow)'}")

    create_tables()

    if tables_are_populated():
        logger.info("Tables already populated - skipping pipeline")
        return

    logger.info("Fetching raw data from Socrata...")
    try:
        fetch_data()
    except Exception as e:
        logger.error(f"Failed to fetch Socrata data: {e}")
        return

    if USE_LOCAL_RUNNER:
        logger.info("Loading NYC DCA businesses (local runner)...")
        load_nyc_from_gcs()

        logger.info("Loading NYS corporations (local runner)...")
        load_nys_from_gcs()
    else:
        logger.info("Loading data via Apache Beam (Dataflow)...")
        run_beam_pipelines()

    logger.info("Running fuzzy matching...")
    anomalies = run_fuzzy_matching()

    write_anomalies(anomalies)

    logger.info("=== execution.py complete ===")


if __name__ == "__main__":
    run()