# KYB Compliance Pipeline

This project is a Know Your Business (KYB) compliance pipeline. It cross-references NYC business licenses (fetched from Socrata) against New York State (NYS) corporate entities to detect compliance anomalies, such as dissolved entities still holding active licenses, address mismatches, or entities formed after their license was issued.

## Architecture & Tech Stack

The project is split into three main components:

*   **Data Pipeline (`pipeline/` and `backend/`)**: 
    *   Downloads raw CSV datasets from Socrata directly to Google Cloud Storage (GCS).
    *   Uses Apache Beam to stream, process, and validate the data (via Pydantic) into a PostgreSQL database.
    *   Runs fuzzy matching (optimized by zip code pre-filtering) to compare NYC businesses against NYS entities and compute anomaly flags.
*   **Backend API (`backend/api.py`)**: 
    *   A FastAPI application that queries the processed anomaly and entity data from Postgres.
*   **Frontend (`frontend/`)**: 
    *   A Next.js dashboard to visualize the anomalies and compliance results.

## Prerequisites

Before you begin, ensure you have the following installed and configured:
*   Docker and Docker Compose
*   **GCP Credentials**: Save a valid Google Cloud service account key as `gcp-credentials.json` inside the `backend/` directory.

## Getting Started

The entire application is containerized with Docker Compose. To spin up the database, run the initial data pipeline, and start the API and frontend, run:

```bash
docker-compose up --build
```

Wait for the ingestion pipeline to complete to view the compliance metrics. Once everything is running, access the services at:
*   **Frontend Dashboard**: http://localhost:3000
*   **Backend API (Swagger UI)**: http://localhost:8080/docs

## Repository Structure

```text
.
├── backend/       # FastAPI application, data fetchers, and pipeline orchestration
├── frontend/      # Next.js UI application
├── pipeline/      # SQL schema and Apache Beam runner logic
└── tests/         # Unit and integration tests
```