# 🚖 NYC Yellow Taxi Big Data Pipeline

An enterprise-grade, scalable data engineering pipeline designed to ingest, process, and analyze **over 13 million records** large-scale urban mobility data. This project implements a **Medallion Architecture** combined with **H3 Geospatial Indexing** to efficiently process millions of Yellow Taxi trips, optimized for spatial analytics and downstream analytics/machine learning workloads.

---

##  Architecture & Tech Stack

The pipeline is built using modern big data tools, ensuring a strict separation of concerns between batch ingestion, simulated streaming, distributed processing, and schema management.

* **Orchestration & Ingestion:** Apache NiFi (Configured for **simulated streaming data flows**, mimicking real-time event streaming using historical batch files).
* **Distributed Processing:** Apache Spark & PySpark (Handling transformations, window functions, and geospatial calculations).
* **Geospatial Analytics:** Uber's H3 Spatial Indexing (Converting latitude/longitude coordinates into hierarchical hexagonal indexes for spatial aggregation).
* **Data Lake Storage (Medallion Architecture):**
  * `Bronze Layer`: Raw immutable data ingested from source.
  * `Silver Layer`: Cleaned, filtered, and spatially enriched data (Parquet format).
  * `Gold Layer`: Aggregated business-level metrics ready for BI and ML consumption.
* **Metadata & DDLs:** Apache Hive / SQL Scripts.

---

## 📊 Dataset & Source

The pipeline processes real-world urban transport records sourced from the official TLC Trip Record Data repository. 
* **Primary Dataset Reference:** [TLC Trip Record Data - Yellow Taxi (Parquet)](https://www.kaggle.com/datasets/marcbrandner/tlc-trip-record-data-yellow-taxi?select=yellow_tripdata_2009-02.parquet)
* **Ingestion Strategy:** While the source format consists of structured Parquet files, our Apache NiFi setup introduces a **simulated streaming mechanism** to stream records incrementally into the Bronze layer, emulating real-world production streaming environments.

---

## 📂 Repository Structure

```text
rides-geospatial-data-pipeline/
│
├── 📁 spark/
│   ├── 📁 notebooks/        # Exploratory Data Analysis (EDA) & prototyping notebooks
│   └── 📁 scripts/          # Production-ready PySpark ETL and processing scripts
│
├── 📁 nifi/                 # NiFi templates, flow definitions, and simulated streaming configurations
│
├── 📁 hive/                 # Hive DDLs, table definitions, and schema management scripts
│
├── 📁 docs/                 # Architecture diagrams & documentation
│
└── 📄 .gitignore            # Excludes heavy datasets, check-points, and local caches
