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

from datetime import date, datetime
from typing import Optional
from dotenv import load_dotenv
from rapidfuzz import fuzz
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import stroage

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
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", 5432),
    )
