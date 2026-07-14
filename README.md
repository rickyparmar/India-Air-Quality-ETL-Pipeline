# 🌍 India Air Quality ETL Pipeline

<p align="center">

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-E25A1C?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?logo=pandas&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

</p>

> **Production-style ETL pipeline that extracts real-time Air Quality Index (AQI) data from the Government of India's data.gov.in API, performs cleaning and transformation, and loads analytics-ready data into PostgreSQL using Apache Airflow and Docker.**

---

## 🚀 Highlights

- 📡 Extracts real-time AQI data from **data.gov.in**
- 🔄 Automated daily ETL using **Apache Airflow**
- 🐳 Fully Dockerized development environment
- 🗄️ Loads cleaned data into **PostgreSQL**
- 📂 Archives raw API responses for auditing
- 🔁 Idempotent upserts using `ON CONFLICT DO UPDATE`
- 📈 Analytics-ready dataset
- ⚡ Production-inspired project structure

---

## 📸 Project Preview

> Screenshots will be added after deployment and end-to-end testing.

The following visuals will be included:

- Airflow DAG
- Airflow Graph View
- PostgreSQL Database Tables
- Docker Containers
- ETL Pipeline Execution

---

## 🏗️ System Architecture

```text
                    +----------------------+
                    | data.gov.in API      |
                    | (Real-time AQI Data) |
                    +----------+-----------+
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

## 📚 Table of Contents

- [📖 Project Overview](#project-overview)
- [✨ Features](#features)
- [🏗️ Architecture](#architecture)
- [📂 Project Structure](#project-structure)
- [🌍 Data Source](#data-source)
- [💻 Tech Stack](#tech-stack)
- [⚙️ ETL Workflow](#etl-workflow)
- [🗄️ Database Design](#database-design)
- [🌬️ Airflow](#airflow)
- [🗃️ Why Two PostgreSQL Databases?](#why-two-postgresql-databases)
- [🧹 Data Quality Challenges](#data-quality-challenges)
- [🎯 Design Decisions](#design-decisions)
- [🚀 Setup](#setup)
- [📊 Example Analytical Queries](#example-analytical-queries)
- [🔮 Future Improvements](#future-improvements)
- [🎓 Skills Demonstrated](#skills-demonstrated)
- [📄 License](#license)

---
