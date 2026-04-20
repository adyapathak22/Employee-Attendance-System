# Task 24 — Cloud Basics: AWS vs GCP vs Azure for Data Engineering

## Service Comparison Matrix

| Category | AWS | GCP | Azure |
|----------|-----|-----|-------|
| Object Storage | S3 | Cloud Storage (GCS) | Azure Blob Storage |
| Data Warehouse | Redshift | BigQuery | Synapse Analytics |
| Managed Spark | EMR | Dataproc | HDInsight / Databricks |
| Stream Processing | Kinesis | Pub/Sub + Dataflow | Event Hubs + Stream Analytics |
| ETL/Orchestration | Glue | Cloud Composer (Airflow) | Data Factory |
| NoSQL Database | DynamoDB | Firestore / Bigtable | Cosmos DB |
| Relational DB | RDS / Aurora | Cloud SQL / AlloyDB | Azure SQL / PostgreSQL |
| Data Catalog | Glue Data Catalog | Data Catalog | Purview |
| Serverless Compute | Lambda | Cloud Functions | Azure Functions |
| Container Orchestration | EKS | GKE | AKS |
| ML Platform | SageMaker | Vertex AI | Azure ML |
| BI / Dashboards | QuickSight | Looker Studio | Power BI |

---

## Attendance System — Recommended Cloud Architecture

### Option A: AWS
```
S3 (raw) → Glue ETL → Redshift (warehouse) → QuickSight (dashboard)
          ↓
     Kinesis → Lambda → DynamoDB (real-time alerts)
```

### Option B: GCP
```
GCS (raw) → Dataflow → BigQuery (warehouse) → Looker Studio (dashboard)
           ↓
      Pub/Sub → Cloud Functions → Firestore (real-time)
```

### Option C: Azure
```
Blob (raw) → Data Factory → Synapse (warehouse) → Power BI (dashboard)
            ↓
       Event Hubs → Stream Analytics → Cosmos DB (real-time)
```

---

## Cost Estimate (Monthly, 200 employees, 1 year data)

| Service | AWS | GCP | Azure |
|---------|-----|-----|-------|
| Storage (50 GB) | $1.15 | $1.00 | $0.95 |
| Data Warehouse | $25-50 | $0 (on-demand) | $30-60 |
| ETL Jobs | $5-15 | $3-10 | $5-20 |
| Streaming | $10-20 | $5-15 | $10-25 |
| **Total Estimate** | **$50-100** | **$20-40** | **$50-110** |

> GCP BigQuery is cheapest for ad-hoc queries (pay per query, not per hour).

---

## Key Differentiators

**AWS** — Largest ecosystem, most services, best for enterprises already on AWS.
Pros: Mature tooling, huge community, most integrations.
Cons: Complex pricing, vendor lock-in risk.

**GCP** — Best for analytics and ML workloads.
Pros: BigQuery is unmatched for ad-hoc SQL at scale, Dataflow is powerful.
Cons: Smaller ecosystem than AWS, less enterprise adoption.

**Azure** — Best if organization uses Microsoft stack (Office 365, Teams).
Pros: Deep Active Directory integration, Power BI is best-in-class BI tool.
Cons: Some services lag behind AWS/GCP in maturity.
