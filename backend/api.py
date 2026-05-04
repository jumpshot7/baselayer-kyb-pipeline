"""
api.py

FastAPI backend for the KYB Compliance Detection Pipeline. Provides endpoints to query
NYC DCA businesses, NYS corporations, and the fuzzy-matched anomaly results.

Run with: uvicorn api:app --host 0.0.0.0 --port 8080 --reload
"""

import logging
import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi. middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# App Setup
# FastAPI() create the app instance
# CORSMiddleware allows Next.js frontend (running on localhost:3000) to call this API (on localhost:8080)
# without the browser blocking it due to CORS policy.
app = FastAPI(
    title ="KYB Compliance API",
    description="Anomaly detection across NYC DCA licenses and NY State corporate entities",
    verision="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://laughing-space-sniffle-wjx466vrj56cw5g-3000.app.github.dev",
        "https://kyb-pipeline.vercel.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database Connection
# Same pattern as execution.py - reads from .env
# psycopg2.extras.RealDictCursor returns rows as dicts
# instead of tuples, so we get {"id": 1, "name": "..."}
# instead of (1, "...") - much easier to work with
def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

def query(sql: str, params=None) -> list[dict]:
    """
    Run a SQL query and return all rows as a list of dicts. Uses RealDictCursor so column names are preserved.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def query_one(sql: str, params=None) -> Optional[dict]:
    """
    Run a SQL query and return a single row as a dict, or None if no result.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()

# Response Models
# Pydantic models for API responses. These define the shape of what the API returns
# FastAPI uses them to auto-generate the /docs schema.
class AnomalySummary(BaseModel):
    total_anomalies: int
    flag_license_active_entity_dissolved: int
    flag_license_predates_formation: int
    flag_entity_dormant: int
    flag_address_mismatch: int

# Health Check
# Always the first endpoint. Lets Docker, monitoring tools, and the frontend verify the API is alive and the DB connection is working
@app.get("/health")
def health():
    try:
        result = query_one("SELECT COUNT(*) as count FROM kyb_anomalies")
        return{
            "status": "ok",
            "anomaly_count": result["count"] if result else 0,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Anomaly Endpoints
@app.get("/anomalies/summary")
def get_anomaly_summary():
    """
    Returns counts of each anomaly type.
    Perfect for the dashboard overview cards.
    """
    result = query_one("""
        SELECT
            COUNT(*) AS total_anomalies,
            COUNT(*) FILTER (WHERE flag_license_active_entity_dissolved) AS flag_license_active_entity_dissolved, 
            COUNT(*) FILTER (WHERE flag_license_predates_formation) AS flag_license_predates_formation,
            COUNT(*) FILTER (WHERE flag_entity_dormant) AS flag_entity_dormant,
            COUNT(*) FILTER (WHERE flag_address_mismatch) AS flag_address_mismatch
        FROM kyb_anomalies
        WHERE has_anomaly = TRUE
    """)
    return result

@app.get("/anomalies")
def get_anomalies(
    has_anomaly: bool = Query(True),
    flag_dissolved: bool = Query(False),
    flag_predates: bool = Query(False),
    flag_dormant: bool = Query(False),
    flag_address: bool = Query(False),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Returns anomalies with full NYC + NYS details joined.
    Filterable by specific flag types.
    Paginated via limit/offset.
    """
    # Build WHERE clause dynamically based on query params
    conditions = ["a.has_anomaly = %(has_anomaly)s"]
    params = {"has_anomaly": has_anomaly, "limit": limit, "offset": offset}

    if flag_dissolved:
        conditions.append("a.flag_license_active_entity_dissolved = TRUE")
    if flag_predates:
        conditions.append("a.flag_license_predates_formation = TRUE")
    if flag_dormant:
        conditions.append("a.flag_entity_dormant = TRUE")
    if flag_address:
        conditions.append("a.flag_address_mismatch = TRUE")
    
    where = " AND ".join(conditions)

    results = query(f"""
        SELECT
            a.id,
            a.match_score,
            a.flag_license_active_entity_dissolved,
            a.flag_license_predates_formation,
            a.flag_entity_dormant,
            a.flag_address_mismatch,
            a.has_anomaly,
            a.created_at,
            
            -- NYC business fields
            n.license_number,
            n.business_name,
            n.license_status,
            n.license_type,
            n.expiration_date,
            n.borough,
            n.zip_code AS nyc_zip,
                    
            -- NYS entity fields
            e.dos_id,
            e.current_entity_name,
            e.initial_dos_filing_date AS date_of_formation,
            e.zip_code AS nys_zip
            
        FROM kyb_anomalies a
        JOIN nyc_dca_businesses n ON n.id = a.nyc_business_id
        JOIN nys_corp_entities e ON e.id = a.nys_entity_id
        WHERE {where}
        ORDER BY a.match_score DESC
        LIMIT %(limit)s OFFSET %(offset)s
        """, params)
    return{"results": results, "count": len(results), "offset": offset}

@app.get("/anomalies/dissolved")
def get_dissolved_anomalies(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Active NYC licenses where the NYS entity is dissolved.
    The core KYB red flag - business is operating but its legal entity no longer exists.
    """
    results = query("""
        SELECT
            n.business_name,
            n.license_number,
            n.license_status,
            n.expiration_date,
            n.borough,
            e.current_entity_name,
            e.date_of_dissolution,
            e.entity_type,
            a.match_score
        FROM kyb_anomalies a
        JOIN nyc_dca_businesses n ON n.id = a.nyc_business_id
        JOIN nys_corp_entities  e ON e.id = a.nys_entity_id
        WHERE a.flag_license_active_entity_dissolved = TRUE
        ORDER BY e.date_of_dissolution DESC
        LIMIT %s OFFSET %s
    """, (limit, offset))

    return {"results": results, "count": len(results)}

@app.get("/anomalies/predates")
def get_predates_anomalies(
    limit:  int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    NYC licenses issued before the NYS entity was formed.
    Possible fraud signal — how was a license issued for
    an entity that didn't exist yet?
    """
    results = query("""
        SELECT
            n.business_name,
            n.license_number,
            n.initial_issuance_date,
            e.current_entity_name,
            e.date_of_formation,
            a.match_score
        FROM kyb_anomalies a
        JOIN nyc_dca_businesses n ON n.id = a.nyc_business_id
        JOIN nys_corp_entities  e ON e.id = a.nys_entity_id
        WHERE a.flag_license_predates_formation = TRUE
        ORDER BY a.match_score DESC
        LIMIT %s OFFSET %s
    """, (limit, offset))

    return {"results": results, "count": len(results)}

@app.get("/anomalies/by-borough/{borough}")
def get_anomalies_by_borough(borough: str, limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    count_result = query_one("""
        SELECT COUNT(*) as count
        FROM kyb_anomalies a
        JOIN nyc_dca_businesses n ON n.id = a.nyc_business_id
        WHERE a.has_anomaly = TRUE
        AND LOWER(n.borough) = LOWER(%s)
    """, (borough,))
    
    results = query("""
        SELECT n.business_name, n.license_number, n.license_status, n.borough,
               e.current_entity_name, a.match_score, a.has_anomaly,
               a.flag_license_active_entity_dissolved, a.flag_license_predates_formation,
               a.flag_entity_dormant, a.flag_address_mismatch
        FROM kyb_anomalies a
        JOIN nyc_dca_businesses n ON n.id = a.nyc_business_id
        JOIN nys_corp_entities e ON e.id = a.nys_entity_id
        WHERE a.has_anomaly = TRUE AND LOWER(n.borough) = LOWER(%s)
        ORDER BY a.match_score DESC
        LIMIT %s OFFSET %s
    """, (borough, limit, offset))

    return {"results": results, "count": count_result["count"], "borough": borough}

@app.get("/anomalies/{anomaly_id}")
def get_anomaly_by_id(anomaly_id: int):
    """
    Full detail for a single anomaly record.
    Joins all fields from both source tables.
    """
    result = query_one("""
        SELECT
            a.*,
            row_to_json(n) AS nyc_business,
            row_to_json(e) AS nys_entity
        FROM kyb_anomalies a
        JOIN nyc_dca_businesses n ON n.id = a.nyc_business_id
        JOIN nys_corp_entities  e ON e.id = a.nys_entity_id
        WHERE a.id = %s
    """, (anomaly_id,))

    if not result:
        raise HTTPException(status_code=404, detail=f"Anomaly {anomaly_id} not found")

    return result

# Business Search Endpoints

@app.get("/businesses/search")
def search_businesses(
    name:   str = Query(..., min_length=2),
    limit:  int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    Search NYC businesses by name using ILIKE (case insensitive).
    e.g. /businesses/search?name=joes+pizza
    The % wildcards mean "contains this string anywhere"
    """
    results = query("""
        SELECT
            id, license_number, business_name,
            license_status, license_type, business_category,
            expiration_date, borough, zip_code
        FROM nyc_dca_businesses
        WHERE business_name ILIKE %s
        ORDER BY business_name
        LIMIT %s OFFSET %s
    """, (f"%{name}%", limit, offset))

    return {"results": results, "count": len(results), "query": name}

@app.get("/businesses/{license_number}")
def get_business_by_license(license_number: str):
    """
    Full detail for a single NYC DCA business.
    Also returns any anomalies associated with it.
    """
    business = query_one("""
        SELECT * FROM nyc_dca_businesses
        WHERE license_number = %s
    """, (license_number,))

    if not business:
        raise HTTPException(
            status_code=404,
            detail=f"Business with license {license_number} not found"
        )

    # Also fetch any anomalies linked to this business
    anomalies = query("""
        SELECT
            a.match_score,
            a.has_anomaly,
            a.flag_license_active_entity_dissolved,
            a.flag_license_predates_formation,
            a.flag_entity_dormant,
            a.flag_address_mismatch,
            e.current_entity_name,
            e.entity_type,
            e.date_of_dissolution
        FROM kyb_anomalies a
        JOIN nys_corp_entities e ON e.id = a.nys_entity_id
        WHERE a.nyc_business_id = %s
        ORDER BY a.match_score DESC
    """, (business["id"],))

    return {"business": business, "anomalies": anomalies}

# Entity Endpoints

@app.get("/entities/{dos_id}")
def get_entity_by_dos_id(dos_id: str):
    """
    Full detail for a single NYS corporation/entity.
    Also returns any anomalies associated with it.
    """
    entity = query_one("""
        SELECT * FROM nys_corp_entities
        WHERE dos_id = %s
    """, (dos_id,))

    if not entity:
        raise HTTPException(
            status_code=404,
            detail=f"Entity with DOS ID {dos_id} not found"
        )

    anomalies = query("""
        SELECT
            a.match_score,
            a.has_anomaly,
            a.flag_license_active_entity_dissolved,
            a.flag_license_predates_formation,
            a.flag_entity_dormant,
            a.flag_address_mismatch,
            n.business_name,
            n.license_number,
            n.license_status
        FROM kyb_anomalies a
        JOIN nyc_dca_businesses n ON n.id = a.nyc_business_id
        WHERE a.nys_entity_id = %s
        ORDER BY a.match_score DESC
    """, (entity["id"],))

    return {"entity": entity, "anomalies": anomalies}