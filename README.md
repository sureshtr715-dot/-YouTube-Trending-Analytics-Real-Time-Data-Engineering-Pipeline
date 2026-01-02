📊 YouTube Trending Analytics – Real-Time Data Engineering Pipeline
---
[![Status](https://img.shields.io/badge/status-reviewed-brightgreen.svg)]() [![Stack](https://img.shields.io/badge/tech-Spark%20|%20Kafka%20|%20Airflow%20|%20Streamlit-blue.svg)]() [![License](https://img.shields.io/badge/license-MIT-green.svg)]()

## Project Overview

This project is an end-to-end **real-time + batch data engineering pipeline** that ingests YouTube trending video data, processes it using **Kafka and Spark Structured Streaming**, stores it in **Delta Lake (Bronze, Silver, Gold)**, orchestrates transformations with **Apache Airflow**, and visualizes analytics using **Streamlit**.

The project demonstrates **modern data engineering best practices** including streaming ingestion, medallion architecture, batch orchestration, and analytics-ready dashboards.

---

## 🛠 Tech Stack

* **Language:** Python 3.10
* **Streaming:** Apache Kafka, ZooKeeper
* **Processing:** Apache Spark (Structured Streaming & Batch)
* **Storage:** Delta Lake (Bronze, Silver, Gold)
* **Orchestration:** Apache Airflow
* **Metadata DB:** PostgreSQL (Airflow)
* **Visualization:** Streamlit
* **API:** YouTube Data API v3

---

## 📁 Project Structure

```text
youtube-trending-analytics/
│
├── app/                    # Kafka producer
├── spark_jobs/             # Spark ETL jobs
├── dags/                   # Airflow DAGs
├── streamlit_app/          # Streamlit dashboard
├── logs/                   # Runtime logs (ignored in Git)
├── delta-lake/             # Bronze / Silver / Gold tables (ignored)
├── requirements/           # Dependency files per environment
├── start_pipeline.sh       # Pipeline startup script
└── README.md
```

---

## 🏗 Architecture Overview

The pipeline follows a **Medallion Architecture (Bronze → Silver → Gold)**:

1. **Kafka Producer** fetches trending videos from YouTube API
2. **Kafka Topic** buffers real-time streaming data
3. **Spark Structured Streaming** consumes Kafka data → Delta Bronze
4. **Spark Batch Jobs** transform Bronze → Silver → Gold
5. **Apache Airflow** orchestrates batch workflows
6. **Streamlit Dashboard** reads Gold tables for analytics

![Architecture Diagram](pipeline_architecture.png)

---

## 🧱 Medallion Architecture

* **Bronze:** Raw streaming data (append-only)
* **Silver:** Cleaned, structured, deduplicated data
* **Gold:** Aggregated analytics tables for BI & dashboards

---

## 🔄 Data Flow

```
YouTube API
→ Kafka Producer
→ Kafka Topic (youtube_trending)
→ Spark Streaming
→ Delta Lake (Bronze)
→ Spark Batch Jobs
→ Delta Lake (Silver)
→ Aggregations
→ Delta Lake (Gold)
→ Streamlit Dashboard
```

---

## 🧠 ETL Responsibilities

### Real-Time ETL

* Spark Structured Streaming
* Reads JSON messages from Kafka
* Applies schema & ingestion timestamps
* Writes append-only data to Bronze Delta tables

### Batch ETL

* Orchestrated using Apache Airflow
* Bronze → Silver: cleaning, casting, deduplication
* Silver → Gold: aggregations & analytics prep

### Storage Layer

* **Delta Lake** acts as the analytical data store
* Supports ACID, schema evolution, and time travel

---

## 🥇 Gold Layer Tables

* **`trending_leaderboard`** – Top videos by views & likes
* **`channel_performance`** – Channel-level performance metrics
* **`hourly_growth`** – Hourly view growth per video

---

## 🛫 Airflow Orchestration

The pipeline uses **three Airflow DAGs**:

* `yt_bronze_to_silver` – Bronze → Silver
* `yt_silver_to_gold` – Silver → Gold
* `yt_master_pipeline` – Controls execution order

The master DAG ensures **Silver completes before Gold**.

## Airflow DAGs
![Airflow DAGs](screenshorts/airflow_dags.png)

## Airflow DAG Graph
![Airflow DAG Graph](screenshorts/airflow_dag_graph.png)

---

## 📊 Streamlit Dashboard

The Streamlit app visualizes **Gold-layer analytics**:

* Trending leaderboard
* Channel performance metrics
* Hourly growth trends

## Trending Leaderboard
![Trending Leaderboard](screenshorts/streamlit_trending.png)

## Channel Performance 
![Channel Performance](screenshorts/streamlit_channel_performance.png)

## Hourly Growth
![Hourly Growth](screenshorts/streamlit_Hourly_Growth.png)

---

## 🚀 How to Run the Project (Local Setup)

### 1 Prerequisites

Install once:

* Python 3.10
* Java 8 or 11
* Apache Kafka
* Apache Spark
* PostgreSQL

Verify:

```bash
python3 --version
java -version
```

---

### 2 Virtual Environments

Each component runs in an isolated venv.

#### Kafka Producer + Spark

```bash
python3 -m venv yt-venv
source yt-venv/bin/activate
pip install -r requirements/yt-venv.txt
deactivate
```

#### Airflow

```bash
python3 -m venv airflow-venv
source airflow-venv/bin/activate
pip install -r requirements/airflow-venv.txt
deactivate
```

#### Streamlit

```bash
python3 -m venv streamlit-venv
source streamlit-venv/bin/activate
pip install -r requirements/streamlit-venv.txt
deactivate
```

---


### 3 Start Streaming Pipeline

```bash
./start_pipeline.sh
```

This starts:

* ZooKeeper
* Kafka Broker
* Kafka Producer
* Spark Structured Streaming (Bronze)

---

### 4 Run Airflow (Batch Processing)

```bash
source airflow-venv/bin/activate
airflow db init
airflow webserver --port 8080
airflow scheduler
```

Trigger DAG:

```
yt_master_pipeline
```

---

### 5 Run Streamlit Dashboard

```bash
source streamlit-venv/bin/activate
streamlit run streamlit_app/app.py
```

Open:

```
http://localhost:8501
```


## 6 Key Highlights
```
- Real-time streaming with Kafka & Spark
- Medallion architecture using Delta Lake
- Workflow orchestration with Apache Airflow
- End-to-end automation via shell script
- Analytics dashboard built with Streamlit
```


## Future Enhancements
```
- Deploy on cloud (AWS / GCP)
- Add data quality checks
- CI/CD for DAGs
- Alerting and monitoring
```

## 👤 Author

**Suresh Kumar**











