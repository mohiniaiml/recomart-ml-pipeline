# RecoMart – End-to-End Data Management Pipeline

## 📌 Overview
This project implements an end-to-end data management pipeline for a recommendation system.  
It demonstrates ingestion, storage, and processing of multi-source data including transactions, clickstream events, and product metadata.

The pipeline follows a hybrid architecture:
- Batch ingestion for transactions
- Streaming simulation for clickstream data
- API-based ingestion for product metadata

---

## 🏗️ Architecture

### Production Design
- CDC-based ingestion for transactions
- Event streaming for clickstream
- API-based ingestion for product metadata

### Simulation (Implemented)
- CSV-based batch ingestion (transactions)
- JSON log-based streaming simulation (clickstream)
- Mock API ingestion (product metadata)

Refer:
- `docs/architecture_production.png`
- `docs/architecture_simulation.png`

---

## 📂 Project Structure

| Folder | Description |
|-------|------------|
| simulators/ | Generates synthetic data |
| ingestion/ | Ingestion scripts |
| api/ | Mock product API |
| data_lake/ | Raw data storage |
| common/ | Utility functions |
| docs/ | Architecture diagrams |

---

## ⚙️ Setup

### 1. Clone Repository
```bash
git clone https://github.com/your-username/recomart-ml-pipeline.git
cd recomart-ml-pipeline
