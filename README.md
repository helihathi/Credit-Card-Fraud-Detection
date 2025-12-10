# 🌐🔍 Credit Card Fraud Detection — Real-Time ML Pipeline (Docker-only)
### 🚀 End-to-End Streaming System using Kafka, Spark, FastAPI, XGBoost, Streamlit & Docker

> ⚠️ **Important:** This repository runs *only via Docker / Docker Compose*. Manual execution (`python app.py`, `streamlit run`, etc.) is **not supported** for reproducing the end-to-end pipeline. Everything below assumes Docker is available on your machine.

---

## 📌 Overview

This project provides a **real-time credit card fraud detection pipeline** using Kafka for ingestion, Spark Structured Streaming for processing, FastAPI for inference, Streamlit for visualization, and SQLite for persistence—all fully containerized via Docker.

---

## ✅ Quick highlights

- Docker-first: the full stack runs only inside containers.  
- Reproducible: clone → `docker-compose up --build -d` → open dashboard.  
- Model files included: `model/xgboost_caliberated.joblib`, `model/standard_scaler.joblib` (13 MB).

---

## 📁 Folder Structure & Explanation

```
credit-card-fraud-detection/
│
├── data/                                # Dataset + runtime databases
│
├── fastapi/                             # Fraud inference microservice
│   ├── app.py                           # FastAPI endpoints
│   ├── alert_email.py                   # Email alert handler
│   ├── Dockerfile                       # FastAPI container build file
│   └── requirements.txt                 # FastAPI dependencies
│
├── fraud-dashboard/                     # Streamlit dashboard module
│   └── realtime_dashboard.py            # Main dashboard file
│
├── model/                               # ML model artifacts
│   ├── xgboost_caliberated.joblib       # Trained classifier
│   └── standard_scaler.joblib           # Preprocessing scaler
│
├── notebook/                            # Training notebooks
│   └── Credit_Card_Fraud_detection.ipynb
│
├── producer/                            # Kafka transaction generator
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements-producer.txt
│
├── spark-consumer/                      # Spark streaming consumer
│   ├── consumer.py
│   ├── alert_email.py
│   ├── Dockerfile
│   └── consumer-requirements.txt
│
├── .dockerignore                        # Excludes unnecessary files from Docker context
├── .gitignore                           # Files ignored by Git
├── .env                                 # Environment variables (not included in repo)
├── create_db.py                         # Database schema initializer
├── migrate_db.py                        # DB alteration scripts
├── etl.py                               # Loads dataset into DB
├── docker-compose.yml                   # Orchestrates entire pipeline
├── Dockerfile                           # Dashboard Dockerfile
├── Dockerfile.base                      # Cached heavy dependencies layer
├── realtime_dashboard.py                # Legacy dashboard entry
├── fraud_detection.db                   # Local DB created manually
└── requirements.txt                     # Dashboard requirements
```

---


## 🔒 `.env` Template (Use `.env.example` in repo)
Add `.env` to `.gitignore`

---

## 🚀 Step-by-step Docker execution (recommended)

### A — Full stack with docker-compose (one command)

1. Build and start everything:

```bash
docker-compose up --build -d
```

2. Check the containers are running:

```bash
docker-compose ps
```

3. View aggregated logs (follow):

```bash
docker-compose logs -f
```

4. To stop and remove containers (keep volumes):

```bash
docker-compose down
```

5. To stop and remove including volumes (WARNING: this deletes DB data):

```bash
docker-compose down -v
```

---

## 🔁 How to execute the container
## Start Zookeeper + Kafka via docker-compose, then start the stack

First pull the `zookeeper` and `kafka` images from docker using
```bash
# Pull Zookeeper Image from docker
docker pull bitnamilegacy/zookeeper:3.6.2

# Pull Kafka Image from docker
docker pull bitnamilegacy/kafka:3.4.0
```

If your `docker-compose.yml` already defines `zookeeper` and `kafka` services, you can bring them up first and wait, then start others:

```bash
# start only zookeeper first
docker-compose up -d zookeeper

# start only kafka (after 30 seconds of starting zookeeper)
docker compose up -d kafka

# then start the rest (after starting kafka)
docker-compose up -d producer spark-consumer fastapi fraud-dashboard
```

This is advisable just to ensure Kafka is ready before consumers connect.

---

## 🛠 Build individual containers (if you change code)

Rebuild specific services after code changes:

```bash
# rebuild only fastapi and spark-consumer
docker-compose build fastapi spark-consumer

# bring them up
docker-compose up -d fastapi spark-consumer
```

---

- List Docker containers:

```bash
docker ps -a
```

- Remove a single container:

```bash
docker rm -f container_name
```

---

## 🧾 Database notes

- The SQLite DB files are created/managed at runtime in `data/` (or wherever your `docker-compose` mounts the volume). Do **not** commit `.db`, `.db-shm`, or `.db-wal` files to GitHub. Use `create_db.py` + `etl.py` to recreate schema and seed data if needed.

---

## ✅ Health checks & quick verification

1. `docker-compose ps` → ensure services show `Up`.  
2. `docker-compose logs -f spark-consumer` → watch streaming predictions.  
3. Open dashboard: `http://localhost:8501` (or `FASTAPI_PORT` / `DASHBOARD_PORT` in `.env`).  
4. Test FastAPI health endpoint:

```bash
curl http://localhost:8000/health
```

5. Post a sample transaction to FastAPI `/predict` to confirm model loads and responds.

---

## 🔁 Model updates (Docker workflow)

If you retrain the model locally and create new joblib files:

1. Replace the files in `model/` locally.  
2. Rebuild images that bundle the model (typically `spark-consumer` and `fastapi`):

```bash
docker-compose build spark-consumer fastapi
docker-compose up -d spark-consumer fastapi
```

Alternatively, mount `./model` as a volume into the containers so you can swap models without rebuilding.

---

## ⚠️ Troubleshooting tips

- If a consumer cannot connect to Kafka, ensure Kafka advertises the listener reachable by the container (check `KAFKA_ADVERTISED_LISTENERS`).  
- If ports are in use (2181/9092/8000/8501), change them in `.env` and `docker-compose.yml`.  
- If images fail to build due to missing model files, ensure `model/` contains the `.joblib` files or configure auto-download in the Dockerfile.

---

## 🖼 Screenshots

```markdown
<p align="center">
  <img src="docs/dashboard.png" width="700" alt="Dashboard screenshot"/>
</p>
```

---

## 🔗 Dataset (source & download)

This project uses the popular **Kaggle Credit Card Fraud Detection** dataset:

- **Kaggle dataset page:** https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud
---
## 📜 License

This repository uses the **MIT License** (see `LICENSE`).

---
