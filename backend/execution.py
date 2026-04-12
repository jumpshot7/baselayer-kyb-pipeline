"""
execution.py

Orchestrates the full KYB pipeline:
1. Check if DB is already populated (skip if so)
2. Create tables form database.sql
3. Apache Beam pipeline - read GCS CSVs, validate, write to Postgres
4. Fuzzy match NYC businesses against NYS entities
5. Compute anomaly flags
6. Write results to kyb_anomalies
"""

import csv
import io
import logging
import os
import psycopg2
import apache_beam as beam
import asyncio


from datetime import date, datetime
from typing import Optional
from dotenv import load_dotenv
from rapidfuzz import fuzz
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from fetcher import run as fetch_data

from models import(
    NycDcaBusiness,
    NysCorpEntity,
    parse_nyc_record,
    parse_nys_record,
)

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Section 1: Database Connection
# All connection details come from .env
# psycopg2 is the standard Python driver for Postgres

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

# Reads database.sql and execute it against Postgres.

def create_tables():
    sql_path = os.path.join(os.path.dirname(__file__), "../pipeline/database.sql")
    with open(sql_path, "r") as f:
        sql = f.read()
    
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("Tables created succesfully")
    finally:
        conn.close()

# Check if already populated
# If both source tables already have rows, the pipeline has already run. Skip to avoid duplicates.
# This is what Docker uses to decide whether to run the pipeline or go straight to launching the API.
def tables_are_populated() -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM nyc_dca_businesses")
            nyc_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM nys_corp_entities")
            nys_count = cur.fetchone()[0]

        logger.info(f"NYC Rows: {nyc_count} | NYS rows: {nys_count}")
        return nyc_count > 0 and nys_count > 0
    finally:
        conn.close()

# GCS CSV Reader
# Apache Beam's ReadFromText reads line by line.
# We need to read CSV from GCS as a full file
# Use csv.DictReader to parse headers.
# This function downloads the CSV bytes from GCS, and yield one dict per pow - Beam processes each dict as a seperate element in the pipeline.
def read_csv_from_gcs(bucket_name : str, blob_name : str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    content = blob.download_as_bytes()
    reader = csv.DictReader(io.StringIO(content.decode("utf-8")))

    for row in reader:
        yield dict(row)

# Beam DoFn Classes
# Do Function is Beam's unit of work. Single transform step in a pipeline. Each DoFn receives one element and yields 0 or more outputs.
# Parse and ValidateNyc recieves a raw dict (one CSV row) passes it through the Pydantic model. and yields the validated model if it passes, nothing if failed.
class ParseAndValidateNyc(beam.DoFn):
    def process(self, element):
        result = parse_nyc_record(element)
        if result is not None:
            yield result
        else:
            logger.warning(f"Could not parse {element}")

class ParseAndValidateNys(beam.DoFn):
    def process(self, element):
        result = parse_nys_record(element)
        if result is not None:
            yield result
        else:
            logger.warning(f"Could not parse {element}")

# WriteNycToPostgres recieves a validated NycDcaBusiness and inserts it into the nyc_dca_businesses table.
# On conflict do nothing means if the license already exists (unique constraint), just skip it silently.
class WriteNycToPostgres(beam.DoFn):
    def setup(self):
        # set() is called once per worker before processing
        # This is where we open the DB connection
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT", 5432),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        self.batch_count = 0

    def teardown(self):
        # teardown() is called once per worker after processing
        self.conn.close()
    
    def process(self, element: NycDcaBusiness):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO nyc_dca_businesses (
                    license_number, business_name, dba_trade_name,
                    business_unique_id, business_category, license_type,
                    license_status, initial_issuance_date, expiration_date,
                    contact_phone, building_number, street, city, state,
                    zip_code, borough, latitude, longitude
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (license_number) DO NOTHING
            """, (
                element.license_number,
                element.business_name,
                element.dba_trade_name,
                element.business_unique_id,
                element.business_category,
                element.license_type,
                element.license_status,
                element.initial_issuance_date,
                element.expiration_date,
                element.contact_phone,
                element.building_number,
                element.street,
                element.city,
                element.state,
                element.zip_code,
                element.borough,
                element.latitude,
                element.longitude,
            ))
        self.batch_count += 1
        if self.batch_count % 500 == 0:
            self.conn.commit()

    def finish_bundle(self):
        # Commits a batch of rows all at once (much faster!)
        self.conn.commit()

class WriteNysToPostgres(beam.DoFn):
    def setup(self):
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT", 5432),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        self.batch_count = 0

    def teardown(self):
        self.conn.close()

    def process(self, element: NysCorpEntity):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO nys_corp_entities (
                    dos_id, current_entity_name, entity_type,
                    dos_process_name, county, jurisdiction,
                    date_of_formation, date_of_dissolution,
                    street_address, city, state, zip_code
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (dos_id) DO NOTHING
            """, (
                element.dos_id,
                element.current_entity_name,
                element.entity_type,
                element.dos_process_name,
                element.county,
                element.jurisdiction,
                element.date_of_formation,
                element.date_of_dissolution,
                element.street_address,
                element.city,
                element.state,
                element.zip_code,
            ))
        self.batch_count += 1
        if self.batch_count % 500 == 0:
            self.conn.commit()

    def finish_bundle(self):
        # Commits a batch of rows all at once (much faster!)
        self.conn.commit()

# Run Beam Pipelines
# This runs two seperate Beam pipelines- one for each dataset. DirectRunner means means it runs locally on your machine.
# When you move to production we will swap to DataflowRunner to run it on Google Cloud Dataflow.
def run_beam_pipelines():
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    options = PipelineOptions(runner="DirectRunner")

    # NYC Pipeline
    logger.info("Running Beam pipeline for NYC DCA businesses...")
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadNycRow" >> beam.Create(
                list(read_csv_from_gcs(bucket_name, "raw/nyc-dca-businesses.csv"))
                )
            | "ValidateNyc" >> beam.ParDo(ParseAndValidateNyc())
            | "WriteNycToDB" >> beam.ParDo(WriteNycToPostgres())   
        )
    logger.info("NYC pipeline complete")

    # NYS Pipeline
    logger.info("Running Beam pipeline for NYS corporations...")
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadNysRows"     >> beam.Create(
                                    list(read_csv_from_gcs(bucket_name, "raw/nys-corporations.csv"))
                                  )
            | "ValidateNys"     >> beam.ParDo(ParseAndValidateNys())
            | "WriteNysToDB"    >> beam.ParDo(WriteNysToPostgres())
        )
    logger.info("NYS pipeline complete")

# Fuzzy Matching + Anomaly Detection
# Load both tables from Postgres into memory. Group by zipcode to avoid comparing every NYC business against every NYS business/
# Any pair with a match score >= 85 gets anomaly flags computed.

MATCH_THRESHOLD = 85.0

def years_since(d: date) -> float:
    """Return how many years have passed since a given date."""
    return (date.today() - d).days / 365.25

def compute_anomaly_flags(nyc: NycDcaBusiness, nys: NysCorpEntity) -> dict:
    """
    Compute all four anomaly flags for a matched pair.
    Returns a dict of flag name -> bool.
    """

    # Flag 1: License active but entity is dissolved
    flag_dissolved = (
        nyc.license_status is not None and
        nyc.license_status.lower() == "active" and
        nys.date_of_dissolution is not None
    )

    # Flag 2: License issued before entity was formed
    flag_predates = (
        nyc.initial_issuance_date is not None and
        nys.date_of_formation is not None and
        nyc.initial_issuance_date < nys.date_of_formation
    )

    # Flag 3: Entity dormant - active license but entity
    # hasn't dissolved yet. Has been around 3+ years with no sign of acitivity (no dissolution date field)
    flag_dormant = (
        nyc.license_status is not None and
        nyc.license_status.lower() == "active" and
        getattr(nys, "dos_process_name", "") == "Inactive" and
        nys.date_of_dissolution is None and
        nys.date_of_formation is not None and
        years_since(nys.date_of_formation) >= 3
    )

    # Flag 4: Zip code mismatch between license and registered entity address
    flag_address = (
        nyc.zip_code is not None and
        nys.zip_code is not None and
        nyc.zip_code.strip() != nys.zip_code.strip()
    )

    return {
        "flag_license_active_entity_dissolved": flag_dissolved,
        "flag_license_predates_formation": flag_predates,
        "flag_entity_dormant": flag_dormant,
        "flag_address_mismatch": flag_address,
        "has_anomaly": any([
            flag_dissolved,
            flag_predates,
            flag_dormant,
            flag_address,
        ]),
    }

def run_fuzzy_matching():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Load NYC businesses - only fields we need
            cur.execute("""
                SELECT id, business_name, license_status, 
                initial_issuance_date, expiration_date,
                zip_code, latitude, longitude
                FROM nyc_dca_businesses
            """)
            nyc_rows = cur.fetchall()

            # Load NYS entities - only fields we need
            cur.execute("""
                SELECT id, current_entity_name, dos_process_name,
                date_of_formation, date_of_dissolution, zip_code
                FROM nys_corp_entities
                """)
            nys_rows = cur.fetchall()
        logger.info(f"Loaded {len(nyc_rows)} NYC rows and {len(nys_rows)} NYS rows")

        # Group NYS entities by zip code for efficient lookup
        # Instead of comparing every NYC business against all 4M
        # NYS entities, we only compare within the same zip code
        nys_by_zip = {}
        for row in nys_rows:
            zip_code = row[5]
            if zip_code not in nys_by_zip:
                nys_by_zip[zip_code] = []
            nys_by_zip[zip_code].append(row)
        
        anomalies = []

        for nyc_row in nyc_rows:
            nyc_id = nyc_row[0]
            nyc_name = nyc_row[1] or ""
            nyc_zip = nyc_row[6]

            # Only compare against NYS entities in the same zip
            candidates = nys_by_zip.get(nyc_zip, [])

            for nys_row in candidates:
                nys_id = nys_row[0]
                nys_name = nys_row[1] or ""

                # token_sort_ratio handles word order differences
                # "JOES PIZZA LLC" vs "LLC JOES PIZZA" -> 100
                score = fuzz.token_sort_ratio(
                    nyc_name.upper(),
                    nys_name.upper()
                )

                if score >= MATCH_THRESHOLD:
                    # Build lightweight model-like objects for flag computation
                    nyc_obj = type("NYC", (), {
                        "license_status": nyc_row[2],
                        "initial_issuance_date": nyc_row[3],
                        "expiration_date": nyc_row[4],
                        "zip_code": nyc_row[6],
                    })()

                    nys_obj = type("NYS", (), {
                        "dos_process_name": nys_row[2],
                        "date_of_formation": nys_row[3],
                        "date_of_dissolution": nys_row[4],
                        "zip_code": nys_row[5],
                    })()

                    flags = compute_anomaly_flags(nyc_obj, nys_obj)

                    anomalies.append({
                        "nyc_business_id": nyc_id,
                        "nys_entity_id": nys_id,
                        "match_score": score,
                        **flags,
                    })

                logger.info(f"Found {len(anomalies)} matches above threshold {MATCH_THRESHOLD}")
                return anomalies
                
    finally:
        conn.close()

# Write Anomalies to Postgres
def write_anomalies(anomalies: list[dict]):
    if not anomalies:
        logger.info("No anomalies to write")
        return
    
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for a in anomalies:
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
                    a["nyc_business_id"],
                    a["nys_entity_id"],
                    a["match_score"],
                    a["flag_license_active_entity_dissolved"],
                    a["flag_license_predates_formation"],
                    a["flag_entity_dormant"],
                    a["flag_address_mismatch"],
                    a["has_anomaly"],
                ))
        conn.commit()
        logger.info(f"Wrote {len(anomalies)} anomaly records to Postgres")
    finally:
        conn.close()

# Orchestrator
def run():
    logger.info("=== execution.py starting ===")

    # Step 1: Create tables if they don't exist
    create_tables()

    # Step 2: Skip if already populated
    if tables_are_populated():
        logger.info("Tables already populated - skipping pipeline")
        return
    
    # Fetch data from socrata to gcs
    logger.info("Fetching raw data from Socrata...")
    try:
        # Run the async fetcher from synchronous code
        asyncio.run(fetch_data())
    except Exception as e:
        logger.error(f"Failed to fetch Socrata data: {e}")
        return

    # Step 3: Run Beam pipelines to populate source tables
    run_beam_pipelines()

    # Step 4: Fuzzy match + compute anomaly flags
    anomalies = run_fuzzy_matching()
    
    # Step 5: Write anomalies to Postgres
    write_anomalies(anomalies)

    logger.info("=== execution.py complete ===")

# Entry Point
if __name__ == "__main__":
    run()