# Montreal BIXI Real-Time Monitoring Platform

### [View Live Dashboard](#) *(Insert link if active)*

A full-stack data engineering project designed to track, archive, and visualize the status of Montreal's BIXI bike-share network in real-time. Built to assist commuters during the 2025 Montreal Transit Strike by identifying system availability trends and station-specific stress metrics.

## 🏗 Architecture

The system moves from a local script-based approach to a fully cloud-native architecture hosted on AWS.

```mermaid
graph LR
    A[BIXI API (GBFS)] -->|JSON| B[AWS Lambda]
    B -->|Docker Image| C[AWS ECR]
    B -->|Write Processed Data| D[(AWS RDS PostgreSQL)]
    D -->|SQL Queries| E[Streamlit Dashboard]
    E -->|Visuals| F[End User]
```

### High-Level Data Flow

1. **Ingestion (ETL):** A Python script packaged in a **Docker** container runs on **AWS Lambda** every 5 minutes. It fetches live data from the BIXI GBFS feed, transforms it using **Polars**, and calculates system-wide aggregates.
2. **Storage:** Data is persisted in a **PostgreSQL** database hosted on **AWS RDS**. The schema is normalized into `station_status_log` (granular history) and `system_aggregate_log` (macro trends).
3. **Visualization:** **Streamlit** dashboard queries the database in real-time to render availability maps, time-series charts, and KPI metrics.

## Key Features

* **Real-Time Dashboard:** Displays total system availability and "Stress Metrics" (number of completely empty or full stations).
* **Historical Tracking:** Tracks availability over a rolling 24-hour window to identify commute patterns.
* **Station-Level Granularity:** Interactive map where clicking a marker triggers a SQL query to fetch and graph that specific station's history over the last 6 hours.

### 2. Automated Serverless Pipeline

The project was migrated from a local cron job to a serverless architecture.

* **Docker & ECR:** The ETL scripts are containerized to ensure consistent dependencies (Polars, Psycopg2).
* **Lambda:** The container is deployed to AWS Lambda with a 5-minute EventBridge trigger.

### 3. Schema Design

The database handles high-frequency writes (approx. 1,000 rows every 5 minutes).

* `station_status_log`: Stores raw station states (lat, lon, capacity, bikes_available).
* `system_aggregate_log`: Stores computed metrics per time-step to reduce compute load on the dashboard side (pre-aggregation).
