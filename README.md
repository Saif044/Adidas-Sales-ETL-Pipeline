# 👟 Adidas Sales ETL Pipeline

A professional Data Engineering pipeline designed to automate the ingestion, cleaning, and transformation of raw Adidas sales data using **Apache Airflow**, **PostgreSQL**, and **Python**.



## 🚀 Project Overview
This project addresses the real-world challenge of processing financial data that arrives with non-numeric formatting. Raw records in this dataset include currency symbols (`$`), commas, and percentage signs (`%`) that block direct database arithmetic. 

The pipeline automates a three-stage **ETL** process:
1.  **Extract**: Pulls "messy" data from a PostgreSQL staging table.
2.  **Transform**: Uses **Pandas** to sanitize strings and calculate a custom **Profit per Unit** KPI.
3.  **Load**: Bulk-inserts refined insights into a final summary table for business reporting.

---

## 🛠️ Tech Stack
* **Orchestration**: Apache Airflow (Astro Runtime)
* **Database**: PostgreSQL 18
* **Processing**: Python 3.12 (Pandas)
* **Infrastructure**: Docker & Astro CLI

---

## 📁 Repository Structure
```text
├── dags/
│   └── adidas_etl_pipeline.py    # Main Airflow DAG and cleaning logic
├── sql/
│   └── schema.sql                # Table definitions (Raw vs. Summary)
├── Dockerfile                    # Image build with Postgres providers
├── requirements.txt              # Python dependencies (pandas, etc.)
├── .gitignore                    # Excludes local logs and secrets
└── README.md                     # Project documentation