# 🚖 NYC Yellow Taxi Big Data Pipeline

A scalable data engineering pipeline designed to ingest, process, and analyze **over 13 million records** large-scale urban mobility data. This project implements a **Medallion Architecture** combined with **H3 Geospatial Indexing** to efficiently process millions of Yellow Taxi trips, optimized for spatial analytics and downstream analytics/machine learning workloads.

---

##  Architecture & Tech Stack

The pipeline is built using modern big data tools, ensuring a strict separation of concerns between batch ingestion, simulated streaming, distributed processing, and schema management.

<img width="3522" height="2056" alt="finaallll" src="https://github.com/user-attachments/assets/704282e8-df49-4462-ba53-e85599fc8c46" />

---
## Pipeline Components & Tech Stack

* **Orchestration & Ingestion (Apache NiFi):** Continuously monitors incoming directories using timestamp tracking and file filters to ingest batches safely. Features built-in back pressure controls, automated Hive partition repair (`MSCK REPAIR TABLE`), and robust retry/quarantine mechanisms for handling transient cluster failures.
* **Distributed Processing (Apache Spark & PySpark):** Handles data cleansing, validation, and incremental transformations via `nyc-medallian-etl.py`. Automatically filters out invalid coordinates, zero passenger counts, and zero-distance trips.
* **Geospatial Analytics (Uber H3 Indexing):** Converts latitude and longitude coordinates into hierarchical hexagonal indexes at **Resolution 8** via custom Python UDFs, alongside optimized great-circle **Haversine distance** calculations.
* **Storage Layers (Medallion Architecture):**
  * **Bronze Layer:** Raw immutable data ingested from source batches into HDFS.
  * **Silver Layer:** Cleaned, filtered, and spatially enriched trip-level data stored in Parquet format and partitioned by year and month.
  * **Gold Layer:** Pre-aggregated spatial-temporal business summaries grouped by geo-hash and pickup hour to track total trips, fare revenues, and metrics.
* **Machine Learning Pipeline (`nyc_taxi_ml_pipeline.py`):** A production-ready regression pipeline supporting Gradient-Boosted Trees (`gbt`), Random Forest (`rf`), and Linear Regression (`lr`) to predict trip durations and fare amounts using strict chronological data splitting.
---
## 💡 Data Engineering Highlights & Optimizations

As an enterprise-grade data engineering pipeline, several advanced optimizations, architectural patterns, and scaling strategies were implemented to ensure high performance, fault tolerance, and efficiency:

* **Distributed Processing & Partitioning:** Utilized Apache Spark with dynamic partition pruning and optimized Snappy compression on Silver and Gold layers, drastically reducing I/O bottlenecks and query scan times in HDFS and Hive.
* **Geospatial Optimization & UDF Efficiency:** Leveraged Uber’s H3 hexagonal hierarchical indexing (Resolution 8) to convert high-cardinality lat/long pairs into compact spatial keys, enabling fast and scalable spatial aggregations without degrading spatial resolution.
* **Fault Tolerance & Back Pressure Control:** Configured Apache NiFi data flows with strict back pressure ceilings (capped at 200 FlowFiles) and automated `RetryFlowFile` mechanisms to gracefully manage network and cluster hiccups, routing failed states to dedicated quarantine boxes.
* **Chronological Data Leakage Prevention:** Engineered the Spark ML pipeline to enforce strict time-ordered data splits (train, validation, test based on explicit timestamps) rather than random splitting, mimicking true production forecasting behavior.
* **Scalable Storage Design (Medallion Architecture):** Implemented strict separation of concerns across Bronze (raw, immutable), Silver (cleaned, typed, and partitioned), and Gold (pre-aggregated business metrics) layers to support concurrent analytical workloads and Tableau dashboards efficiently.
* **Collaborative Infrastructure & Remote Architecture:** Overcame the classic "it works on my machine" barrier by centralizing the data infrastructure, configuring static local IPs, port forwarding, and a No-IP Dynamic DNS hostname. This transformed isolated local environments into a unified, accessible shared testing ground for the entire team.
---
## Dataset & Source

The pipeline processes real-world urban transport records sourced from the official TLC Trip Record Data repository. 
* **Primary Dataset Reference:** [TLC Trip Record Data - Yellow Taxi (Parquet)](https://www.kaggle.com/datasets/marcbrandner/tlc-trip-record-data-yellow-taxi?select=yellow_tripdata_2009-02.parquet)
* **Ingestion Strategy:** While the source format consists of structured Parquet files, our Apache NiFi setup introduces a **simulated streaming mechanism** to stream records incrementally into the Bronze layer, emulating real-world production streaming environments.

---

## Repository Structure

```text
rides-geospatial-data-pipeline/
├── 📂 spark/
│   ├──📂 notebooks/          # Exploratory Data Analysis (EDA) & prototyping notebooks
│   └──📂 scripts/            # Production-ready PySpark ETL (`nyc-medallian-etl.py`) and ML scripts (`nyc_taxi_ml_pipeline.py`)
├── 📂 nifi/                   # NiFi templates, flow definitions, and simulated streaming configurations
├──📂 hive/                   # Hive DDLs, table definitions, and schema management scripts
├──📂 tableau/                 # Tableau dashboards (`nyc dashboard.twb`)
    └── README.md             # Key insights & visualization photos
├──📂 docs/                   # Architecture diagrams & documentation
└── .gitignore              # Excludes heavy datasets, check-points, and local caches
