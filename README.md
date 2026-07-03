India Air Quality ETL Pipeline

A local, Dockerized ETL pipeline that extracts real-time Air Quality Index (AQI) readings from the data.gov.in API, cleans and transforms the data, and loads it into PostgreSQL — orchestrated daily by Apache Airflow.

Problem Statement

Air quality monitoring in India is fragmented across thousands of stations reporting in real time, but the raw API data isn't analysis-ready — it needs deduplication, type correction, and reshaping before it's useful. This pipeline automates that process, producing a clean, queryable dataset that could support use cases like identifying which states/cities show the most volatile pollutant levels, useful for public health monitoring or policy prioritization.

Architecture

data.gov.in API (paginated)
│
▼
extract*task ── writes raw JSON per page to /data/raw/run*{timestamp}/
│
▼
transform_task ── flattens, casts types, dedupes, builds composite key
│ writes cleaned CSV to /data/processed/
▼
load_task ── upserts into PostgreSQL (ON CONFLICT DO UPDATE)
│
▼
analytics-db (Postgres) ── air_quality_readings table

Orchestrated by an Airflow DAG (india_air_quality_etl), scheduled daily, running entirely inside Docker containers alongside two isolated Postgres instances:

airflow-db — Airflow's own metadata/scheduling database
analytics-db — the target database holding the actual AQI data

Keeping these separate mirrors real-world production setups, where orchestration metadata and analytical targets shouldn't share infrastructure — a schema change or backup/restore on one shouldn't risk the other.

Data Source

API: Real-time Air Quality Index from various locations — Central Pollution Control Board (CPCB), via data.gov.in
Why this source: Unlike a static Kaggle CSV, this is a live government API with continuously updating readings from ~3,500+ monitoring records across India, which makes the pipeline genuinely useful to re-run on a schedule rather than a one-off exercise.

Fields returned per record: country, state, city, station, last_update, latitude, longitude, pollutant_id, min_value, max_value, avg_value

Setup & Run Instructions

Prerequisites

Docker Desktop installed and running
A free API key from data.gov.in (register at data.gov.in, then open the AQI dataset's "Data API" page to get your key)

Steps

Clone this repo and navigate into the project folder:

bash cd india_air_quality_etl

Copy the environment template and fill in your credentials:

bash cp .env.example .env

Edit .env and set:

API_KEY=your_data_gov_in_key
POSTGRES_USER=...
POSTGRES_PASSWORD=...
POSTGRES_DB=air_quality
AIRFLOW**WEBSERVER**SECRET_KEY=your_generated_secret_key

Generate a secret key with:

bash python3 -c "import secrets; print(secrets.token_hex(16))"

Build and start all containers:

bash docker compose up --build

Open the Airflow UI at http://localhost:8080 and trigger the india_air_quality_etl DAG manually (or wait for the daily schedule).
Verify data landed correctly:

bash docker exec -it <analytics-db-container> psql -U <user> -d air_quality
SELECT COUNT(\*) FROM air_quality_readings;

Data Quality Issues Found and How I Handled Them

Numeric fields returned as strings. The API returns latitude, longitude, min_value, max_value, and avg_value as strings (e.g., "6" instead of 6). Handled with explicit type casting in transform.py, with invalid/non-numeric values counted and logged rather than silently dropped.
No unique identifier per row. Each row represents one pollutant reading for one station at one timestamp, but the API provides no primary key. Built a composite key from station + pollutant_id + last_update to support safe deduplication and idempotent upserts.
One row per pollutant, not per station. A single station appears in multiple rows — one per pollutant type (PM2.5, PM10, NO2, SO2, CO, NH3) — rather than one row with all pollutants as columns. This shaped how the composite key and downstream queries needed to work.
Live data creates run-to-run variance. Because this is a real-time feed, the exact record count differs slightly between pipeline runs (e.g., one run captured 3,300 records, another 3,337) as monitoring stations report new readings or go offline. This isn't a pipeline bug — it's expected behavior for a live dataset, and it's the reason the load step uses an upsert pattern rather than a full overwrite.
Airflow log retrieval failure (403 Forbidden). Encountered a 403 error when trying to view task logs in the UI, caused by the webserver and scheduler containers each generating a different random secret_key. Fixed by setting a single shared AIRFLOW**WEBSERVER**SECRET_KEY across all Airflow services in .env and docker-compose.yml.

Design Decisions

Upsert over full overwrite: Used INSERT ... ON CONFLICT (composite*key) DO UPDATE so re-running the DAG never creates duplicates, and existing readings get refreshed rather than duplicated — important given the live-data variance noted above.
Raw data archival before transformation: Every raw API page is saved to /data/raw/run*{timestamp}/ before any cleaning happens. This keeps the pipeline auditable — if a transformation bug is ever found, the original data is still there to reprocess.
Two isolated Postgres instances: Airflow's metadata database and the analytics target database are kept fully separate, reflecting standard production practice of not mixing orchestration state with analytical data.
Retry with exponential backoff on extraction: The API is called with up to 3 retries and backoff to handle transient failures or rate limiting without failing the whole pipeline run.

What I'd Improve With More Time

Add a lightweight dashboard (Streamlit or Metabase) on top of analytics-db to visualize state/city-level AQI trends over time
Add automated data quality tests (e.g., Great Expectations) to formally assert schema and value-range expectations on every run, instead of relying on manual log review
Add real alerting (Slack/email) on DAG failure, beyond the current console/log-based failure callback
Historize readings properly (e.g., a is_current flag or SCD pattern) so trend analysis over time is possible, since the current upsert model only keeps the latest value per composite key

Tech Stack

Python · Apache Airflow · PostgreSQL · Docker & Docker Compose · pandas · requests
