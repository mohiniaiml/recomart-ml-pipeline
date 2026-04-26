# RecoMart – End-to-End Data Management Pipeline

## 📌 Overview
This project implements an end-to-end data management pipeline for a recommendation system.  
It simulates real-world data engineering practices by ingesting, storing, and processing data from multiple heterogeneous sources:

- Transactional purchase data (explicit feedback)
- Clickstream user interaction data (implicit behavior)
- Product metadata (catalog data)

The pipeline is designed to be scalable, modular, and reproducible.

---

## 🏗️ Architecture

### Production Design (Conceptual)
- CDC-based ingestion for transactional data
- Event streaming platforms (Kafka/Kinesis)
- API-based ingestion for product metadata

### Simulation (Implemented)
- CSV-based batch ingestion
- JSON log-based streaming
- Mock Flask API

---

## 📂 Project Structure

```
recomart-ml-pipeline/
  ├── src/
    ├── simulators/
    ├── ingestion/
    ├── api/
    ├── common/
  ├── docs/
  ├── requirements.txt
  └── README.md
```

---

## ⚙️ Setup

```
git clone https://github.com/mohiniaiml/recomart-ml-pipeline.git
cd recomart-ml-pipeline
pip install -r requirements.txt
```

---

## 🔄 1. Ingestion
This step ingests data into raw area of data lake

### ▶️ Run

1. Start API:

```
python src/api/product_api.py
```

2. Start simulators:

```
python -m src.simulators.transaction_simulator

python -m src.simulators.clickstream_simulator
```

3. Start ingestion:

```
python -m src.ingestion.ingest_transactions_batch

python -m src.ingestion.ingest_products_api

python -m src.ingestion.process_clickstream_stream
```

### 📊 Output

data_lake/raw/
- transactions/
- products/
- clickstream/

---
## 2. Raw Data Storage
This step moves data from raw to bronze - there is still no pre-preocessing

### ▶️ Run

```
 python -m src.ingestion.run_bronze
```

### 📊 Output

data_lake/bronze/
- source=clickstream/type=raw/year=2026/month=04/day=21/
    - events_001.json
- source=products/type=raw/year=2026/month=04/day=21/
    - products.csv
- source=transactions/type=raw/year=2026/month=04/day=21/
    - tx_001.json

---

## 🧪 3. Data Profiling & Validation

This step validates Bronze data using Great Expectations and generates a PDF report.

### ▶️ Run

```
python -m src.validation.validate_bronze
```

### 📄 Output

```
reports/data_quality_report.pdf
```

---

## ⚪ 4. Silver Layer: Data Preparation & EDA

Transforms Bronze data into cleaned and structured Silver dataset.

### ▶️ Run

```
python -m src.processing.silver_transform
```

### 📊 Output

- Cleaned dataset → data_lake/silver/
- EDA plots → reports/eda/

---

## Feature Engineering Pipeline

Run the scheduled feature pipeline:

```
python -m src.features.scheduler
```

This will:
- Read latest data from data lake
- Generate user, item, and interaction features
- Store features in SQLite database

### 📊 Output
features/features.db

## 📓 Notebook (Recommended)

```
notebooks/silver_eda.ipynb
```

---

## 🔷 Feature Store

▶️ Running Feature Retrieval Demo

```
python -m src.features.store.demo
```

### 📊 Feature Tables
- user_features
- item_features
- interaction_features

---

## 🔷 Model Training

▶️ Run

```
python -m src.models.train_model
```

### 📊 View experiments

```
mlflow ui
```

### Recommendation API

Start API

```
python -m src.api.recommendation_api
```

Call API

```
curl "http://127.0.0.1:5001/recommend?user_id=1&top_n=5"
```

---

## 👩‍💻 Authors
Group 47
