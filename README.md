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

recomart-ml-pipeline/
├── data_lake/
├── simulators/
├── ingestion/
├── api/
├── common/
├── docs/
├── requirements.txt
└── README.md

---

## ⚙️ Setup

git clone https://github.com/mohiniaiml/recomart-ml-pipeline.git
cd recomart-ml-pipeline
pip install -r requirements.txt

---

## 🔄 Running Pipeline

1. Start API:
python api/product_api.py

2. Start simulators:
python simulators/transaction_simulator.py
python simulators/clickstream_simulator.py

3. Start ingestion:
python ingestion/ingest_transactions_batch.py
python ingestion/ingest_products_api.py
python ingestion/process_clickstream_stream.py

---

## 📊 Output

data_lake/raw/
- transactions/
- products/
- clickstream/

---

## 👩‍💻 Authors
Group 47
