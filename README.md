# India Air Quality ETL Pipeline

A production-style, Dockerized ETL pipeline that extracts real-time Air Quality Index (AQI) data from the Government of India's **data.gov.in API**, performs data cleaning and transformation, and loads it into PostgreSQL.

The pipeline is orchestrated daily using **Apache Airflow**, making the dataset continuously updated and analysis-ready.

---

# Project Overview

Air quality data in India is collected from thousands of monitoring stations across the country. Although this information is publicly available through the CPCB API, the raw API response is not directly suitable for analytics because it contains:

- No primary key
- Numeric values stored as strings
- Duplicate records across pipeline runs
- Paginated API responses
- Live data that continuously changes

This project automates the complete ETL workflow and produces a clean PostgreSQL table that can be queried for analytics or connected to BI dashboards.

---

# Features

- Daily scheduled ETL using Apache Airflow
- Extracts live AQI data from data.gov.in
- Handles paginated API responses
- Archives every raw API response
- Cleans and validates incoming data
- Converts incorrect data types
- Removes duplicates
- Generates composite primary keys
- Loads data into PostgreSQL using idempotent upserts
- Fully Dockerized
- Retry mechanism with exponential backoff
- Separation of orchestration and analytics databases

---

# Architecture

```text
                    +----------------------+
                    | data.gov.in API      |
                    | (Real-time AQI Data) |
                    +----------+-----------+
                               |
                               |
                               ▼
                     Extract Task (Airflow)
                               |
                               ▼
          Save Raw JSON (/data/raw/run_timestamp/)
                               |
                               ▼
                   Transform Task (Python)
         - Flatten nested records
         - Type conversion
         - Deduplication
         - Composite key generation
                               |
                               ▼
       Save Clean CSV (/data/processed/)
                               |
                               ▼
                    Load Task (PostgreSQL)
       INSERT ... ON CONFLICT DO UPDATE
                               |
                               ▼
              analytics-db (PostgreSQL)
          air_quality_readings table
```

---

# Project Structure

```text
india_air_quality_etl/

│
├── dags/
│   └── india_air_quality_etl.py
│
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── sql/
│   └── schema.sql
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md

```

---

# Data Source

**Provider**

Central Pollution Control Board (CPCB)

**API**

Real-time Air Quality Index API provided through **data.gov.in**

The API contains approximately **3,500+ monitoring records** collected from monitoring stations across India.

Each record contains:

| Field |
|--------|
| country |
| state |
| city |
| station |
| pollutant_id |
| last_update |
| latitude |
| longitude |
| min_value |
| max_value |
| avg_value |

---

# Tech Stack

- Python
- Apache Airflow
- PostgreSQL
- Docker
- Docker Compose
- pandas
- requests

---

# ETL Workflow

## 1. Extract

- Connects to the data.gov.in API
- Fetches every page of results
- Retries failed requests using exponential backoff
- Saves raw JSON files for auditing

Output:

```
data/raw/run_2026-07-03/
    page_1.json
    page_2.json
    ...
```

---

## 2. Transform

The transformation stage performs:

- Flatten JSON
- Convert numeric strings into numeric types
- Remove duplicates
- Generate composite primary keys
- Validate missing values
- Log invalid records
- Export cleaned CSV

Output:

```
data/processed/air_quality_clean.csv
```

---

## 3. Load

Loads the cleaned dataset into PostgreSQL.

Uses

```sql
INSERT ...
ON CONFLICT (...)
DO UPDATE
```

This ensures:

- No duplicate records
- Safe re-runs
- Idempotent pipeline execution
- Updated readings replace older values

---

# Database Design

## Table

```
air_quality_readings
```

Columns include

- station
- state
- city
- pollutant_id
- last_update
- latitude
- longitude
- min_value
- max_value
- avg_value

Primary Key

```
station
+
pollutant_id
+
last_update
```

---

# Airflow

DAG Name

```
india_air_quality_etl
```

Schedule

```
Daily
```

Pipeline Tasks

```text
extract_task
      │
      ▼
transform_task
      │
      ▼
load_task
```

---

# Why Two PostgreSQL Databases?

The project intentionally uses two isolated PostgreSQL instances.

## airflow-db

Stores

- DAG metadata
- Scheduler state
- Task history
- Logs

## analytics-db

Stores

- Air Quality dataset
- Clean analytical tables

Keeping them separate reflects production best practices where orchestration metadata should never share infrastructure with analytical data.

---

# Data Quality Challenges

## 1. Numeric values returned as strings

Example

```json
"avg_value": "36"
```

Solution

- Explicit type conversion
- Invalid values logged
- Failed conversions counted

---

## 2. No unique identifier

The API provides no primary key.

Solution

Generated composite key using

```
station
+
pollutant_id
+
last_update
```

---

## 3. One row per pollutant

Each station appears multiple times because every pollutant has its own record.

Example

```
Station A
    PM2.5

Station A
    PM10

Station A
    NO2
```

The composite key was designed accordingly.

---

## 4. Live data changes every run

Since the API is real-time,

record counts vary between executions.

Example

```
3304 rows
```

Later

```
3337 rows
```

This is expected behavior rather than a pipeline issue.

---

## 5. Airflow log retrieval issue

Problem

```
403 Forbidden
```

Cause

Different Airflow services generated different `secret_key` values.

Solution

Configured a shared

```
AIRFLOW__WEBSERVER__SECRET_KEY
```

across every Airflow container.

---

# Design Decisions

## Raw Data Archival

Every API response is stored before transformation.

Benefits

- Auditability
- Reprocessing
- Debugging
- Reproducibility

---

## Idempotent Loading

Instead of deleting and reloading data,

the project uses PostgreSQL upserts.

Benefits

- No duplicates
- Safe retries
- Faster incremental updates

---

## Retry Logic

Extraction retries failed requests with exponential backoff.

Benefits

- Handles temporary outages
- Handles API rate limiting
- Prevents unnecessary DAG failures

---

## Containerized Infrastructure

Everything runs inside Docker.

Services include

- Airflow Webserver
- Airflow Scheduler
- Airflow Worker
- airflow-db
- analytics-db

This provides reproducible environments across machines.

---

# Setup

## Prerequisites

- Docker Desktop
- Docker Compose
- data.gov.in API Key

---

## Clone Repository

```bash
git clone https://github.com/your-username/india_air_quality_etl.git

cd india_air_quality_etl
```

---

## Configure Environment

```bash
cp .env.example .env
```

Update

```text
API_KEY=YOUR_API_KEY

POSTGRES_USER=postgres

POSTGRES_PASSWORD=password

POSTGRES_DB=air_quality

AIRFLOW__WEBSERVER__SECRET_KEY=YOUR_SECRET_KEY
```

Generate a secret key

```bash
python -c "import secrets; print(secrets.token_hex(16))"
```

---

## Start the Project

```bash
docker compose up --build
```

---

## Open Airflow

```
http://localhost:8080
```

Trigger

```
india_air_quality_etl
```

---

# Verify Data

Connect to PostgreSQL

```bash
docker exec -it analytics-db psql \
-U postgres \
-d air_quality
```

Example query

```sql
SELECT COUNT(*)
FROM air_quality_readings;
```

---

# Example Analytical Queries

Top 10 cities with highest AQI

```sql
SELECT city,
AVG(avg_value) AS average_aqi
FROM air_quality_readings
GROUP BY city
ORDER BY average_aqi DESC
LIMIT 10;
```

Most monitored pollutants

```sql
SELECT pollutant_id,
COUNT(*)
FROM air_quality_readings
GROUP BY pollutant_id;
```

States with the largest number of monitoring stations

```sql
SELECT state,
COUNT(DISTINCT station)
FROM air_quality_readings
GROUP BY state
ORDER BY COUNT(*) DESC;
```

---

# Future Improvements

- Streamlit dashboard
- Metabase integration
- Great Expectations data validation
- Slack and Email alerts
- Historical versioning (Slowly Changing Dimensions)
- Incremental loading
- Data warehouse star schema
- CI/CD with GitHub Actions
- Unit and integration tests

---

# Skills Demonstrated

- ETL Pipeline Development
- Data Engineering
- Apache Airflow
- PostgreSQL
- Docker
- Data Cleaning
- Data Validation
- API Integration
- Incremental Loading
- SQL
- Python
- Workflow Orchestration
- Production Data Pipeline Design

---

# License

This project is intended for educational and portfolio purposes.
