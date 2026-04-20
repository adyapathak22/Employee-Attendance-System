# 🏢 Employee Attendance System — Advanced Data Engineering Project
## 30 Data Engineering Tasks (End-to-End Pipeline)

---

## 📁 Project Structure

```
employee_attendance_system/
├── data/
│   ├── raw/                   # Raw CSV/JSON source files
│   ├── processed/             # Cleaned & transformed data
│   └── warehouse/             # Star schema output
├── src/
│   ├── ingestion/             # Task 11: Batch ingestion
│   ├── transformation/        # Task 4,5: Python transforms
│   ├── validation/            # Task 29: Data quality
│   ├── streaming/             # Task 18,19,20,21: Kafka + Spark Streaming
│   ├── orchestration/         # Task 22,23: Airflow DAGs
│   └── dashboard/             # Task 30: Final dashboard
├── sql/                       # Task 6,7,8,9: SQL scripts
├── spark/                     # Task 14-17: PySpark jobs
├── kafka/                     # Task 19,20: Kafka scripts
├── airflow/dags/              # Task 22,23: DAG definitions
├── hadoop/                    # Task 12,13: HDFS setup
├── cloud/                     # Task 24-27: Cloud configs
├── scripts/                   # Task 1,2: Linux shell scripts
├── tests/                     # Unit tests
├── notebooks/                 # Jupyter exploration
└── logs/                      # Pipeline logs
```

---

## ⚙️ Prerequisites

```bash
Python 3.10+, Java 11+, Docker, Docker Compose
```

---

## 🚀 Commands to Run (IN ORDER)

### STEP 1 — Environment Setup
```bash
cd employee_attendance_system
python -m venv venv
source venv/bin/activate          # Linux/Mac
# venv\Scripts\activate           # Windows
pip install -r requirements.txt
```

### STEP 2 — Generate Sample Data
```bash
python scripts/generate_data.py
```

### STEP 3 — Linux Setup & Permissions (Task 1)
```bash
bash scripts/setup_linux.sh
```

### STEP 4 — Run ETL Ingestion Pipeline (Task 3, 4, 10, 11)
```bash
python src/ingestion/batch_ingestor.py
python src/transformation/transform_pipeline.py
```

### STEP 5 — Pandas/NumPy Analysis (Task 5)
```bash
python src/transformation/pandas_analysis.py
```

### STEP 6 — SQL Database Setup (Task 6, 7, 8, 9)
```bash
python sql/setup_database.py
python sql/run_queries.py
```

### STEP 7 — Data Warehouse Star Schema (Task 9)
```bash
python sql/star_schema.py
```

### STEP 8 — Data Quality Checks (Task 29)
```bash
python src/validation/data_quality.py
```

### STEP 9 — Spark Jobs (Task 14-17) [Requires Java 11+]
```bash
pip install pyspark
python spark/spark_basic.py
python spark/spark_sql_job.py
python spark/spark_optimized.py
```

### STEP 10 — Kafka Streaming (Task 18-21) [Requires Docker]
```bash
docker-compose up -d
python kafka/producer.py &
python kafka/consumer.py
```

### STEP 11 — Airflow DAGs (Task 22-23) [Requires Airflow]
```bash
pip install apache-airflow
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow scheduler &
airflow webserver --port 8080 &
# Visit http://localhost:8080
```

### STEP 12 — Run Dashboard (Task 30)
```bash
pip install streamlit
streamlit run src/dashboard/app.py
# Visit http://localhost:8501
```

---

## 🧪 Run All Tests
```bash
pytest tests/ -v
```

---

## 📊 Task Coverage Map

| Task | Area | File |
|------|------|------|
| 1 | Linux + File System | scripts/setup_linux.sh |
| 2 | Networking/API | src/ingestion/api_transfer.py |
| 3 | Python CSV/Merge | src/ingestion/csv_merger.py |
| 4 | Python Modules | src/transformation/modules/ |
| 5 | Pandas + NumPy | src/transformation/pandas_analysis.py |
| 6 | SQL Basics | sql/basic_queries.sql |
| 7 | Advanced SQL | sql/advanced_queries.sql |
| 8 | OLTP vs OLAP | sql/oltp_olap_schema.sql |
| 9 | Star Schema | sql/star_schema.py |
| 10 | ETL vs ELT | src/ingestion/etl_elt_compare.py |
| 11 | Batch Ingestion | src/ingestion/batch_ingestor.py |
| 12 | Hadoop/HDFS | hadoop/hdfs_setup.sh |
| 13 | HDFS Architecture | hadoop/hdfs_architecture.md |
| 14 | Spark Basics | spark/spark_basic.py |
| 15 | Spark DataFrames | spark/spark_dataframes.py |
| 16 | Spark SQL | spark/spark_sql_job.py |
| 17 | PySpark Advanced | spark/spark_optimized.py |
| 18 | Streaming Concepts | src/streaming/streaming_sim.py |
| 19 | Kafka Basics | kafka/producer.py + consumer.py |
| 20 | Kafka Advanced | kafka/advanced_kafka.py |
| 21 | Spark Streaming | spark/spark_streaming.py |
| 22 | Airflow Basics | airflow/dags/etl_dag.py |
| 23 | Airflow Advanced | airflow/dags/advanced_dag.py |
| 24 | Cloud Basics | cloud/cloud_compare.md |
| 25 | Cloud Storage | cloud/s3_gcs_upload.py |
| 26 | Cloud Compute | cloud/ec2_deploy.sh |
| 27 | Cloud Warehouse | cloud/bigquery_redshift.py |
| 28 | Lakehouse | cloud/delta_lake.py |
| 29 | Data Quality | src/validation/data_quality.py |
| 30 | Final Pipeline | src/dashboard/app.py |
