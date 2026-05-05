

# Financial_Banking_Data_engineering_project
End-to-End Production-Grade Banking Data Pipeline on AWS

# 🏦 NeoBank AWS Data Engineering Pipeline
### End-to-End Production-Grade Banking Data Pipeline on AWS

[![AWS](https://img.shields.io/badge/AWS-Cloud-orange?logo=amazon-aws)](https://aws.amazon.com)
[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion-purple)](https://www.databricks.com/glossary/medallion-architecture)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Production_Ready-brightgreen)]()




---
## 📌 Project ScreenShots!!

<img width="1408" height="768" alt="Bank_De_ARCH" src="https://github.com/user-attachments/assets/24383b54-e6c9-4268-93a2-5d141b91fb95" />

<img width="1366" height="768" alt="Screenshot from 2026-05-06 00-15-04" src="https://github.com/user-attachments/assets/505f9804-5481-44b9-bdad-1c13e05480fe" />

<img width="1366" height="768" alt="Apache Superset DB" src="https://github.com/user-attachments/assets/7b227ee0-35ac-4407-afe1-f01c9fd75bf9" />
<img width="1366" height="768" alt="Apache Superset DB" src="https://github.com/user-attachments/assets/584c2374-5093-41f8-b6c1-9755763c7360" />

<img width="1366" height="768" alt="Screenshot from 2026-05-01 10-44-13" src="https://github.com/user-attachments/assets/b0106a38-37a5-4037-8247-35fec207da24" />
<img width="1366" height="768" alt="Screenshot from 2026-05-04 19-47-05" src="https://github.com/user-attachments/assets/b5376d30-caf0-4083-871e-89b3da927aa2" />

<img width="1366" height="768" alt="Screenshot from 2026-05-04 19-47-14" src="https://github.com/user-attachments/assets/ee0ae301-3a08-4afe-b5df-7a7830a62806" />


<img width="1366" height="768" alt="Redshit" src="https://github.com/user-attachments/assets/e82be78d-5948-435b-9234-620df6dde38a" />

<img width="1366" height="768" alt="PII Masking" src="https://github.com/user-attachments/assets/967e436c-49c7-4d5a-bf9e-04d72ca6bb5c" />

<img width="1366" height="768" alt="KYC Status" src="https://github.com/user-attachments/assets/c257ffaa-f57a-422c-8850-febded182e22" />


<img width="1080" height="1888" alt="Screenshot from 2026-04-19 13-08-42" src="https://github.com/user-attachments/assets/11c00d81-5c81-4495-8533-6e68a120198c" />
<img width="1080" height="1888" alt="Screenshot from 2026-04-19 13-24-27" src="https://github.com/user-attachments/assets/9784fe1c-2b09-41f1-a49d-66e7f70486a7" />
<img width="1366" height="768" alt="Screenshot from 2026-04-26 12-27-06" src="https://github.com/user-attachments/assets/80464874-8dc4-4607-b89e-e7c47185998f" />



---

## 📌 Project Overview

A **production-grade, end-to-end banking data engineering pipeline** built entirely on AWS, processing **10+ GB of daily data** from multiple heterogeneous sources using the **Medallion Architecture (Bronze → Silver → Gold)**.

This project simulates a real-world **Neo Bank data platform** with full support for:
- ✅ Incremental loads with watermark tracking
- ✅ Data Quality enforcement with quarantine zones
- ✅ PII masking and data governance
- ✅ Star Schema dimensional modeling
- ✅ Automated daily orchestration with retry logic
- ✅ CloudWatch monitoring, anomaly detection, and SNS alerting
- ✅ Complete audit trail and data lineage

---

## 🏗️ Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (6 sources)                       │
├──────────────┬──────────────┬───────────┬───────────┬────────────────┤
│ RDS SQL      │ RDS SQL      │ RDS SQL   │ RDS SQL   │ CSV Files      │
│ Server       │ Server       │ Server    │ Server    │ (S3 Landing)   │
│ branches     │ customers    │ accounts  │ txns      │ payment_gw     │
│ (5 rows)     │ (500+ rows)  │(1000+rows)│(30K+rows) │ credit_bureau  │
└──────┬───────┴──────┬───────┴─────┬─────┴─────┬─────┴──────┬─────────┘
       │              │             │           │            │
       ▼              ▼             ▼           ▼            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE ZONE  (S3 — Raw Parquet)                   │
│   Immutable raw data, 9 metadata columns, versioning ON             │
│   Incremental files appended (never overwritten)                    │
│   Watermark updated per source after each successful run            │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                    6 AWS Glue Visual ETL Jobs
                    + Data Quality Rules (7 per table)
                    + PII Masking (PAN, email, phone)
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SILVER ZONE  (S3 — Clean Parquet)                 │
│   DQ PASSED records only │ Quarantine for failed records            │
│   risk_band enriched     │ dq_status column added                  │
│   Partitioned by load_date                                          │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                    7 AWS Glue Visual ETL Jobs
                    4 Dims + 3 Facts + 4 Aggs
                    Surrogate keys, SCD Type 2
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     GOLD ZONE  (S3 — Star Schema Parquet)            │
│   dim_date  dim_branch  dim_customer  dim_account                   │
│   fact_transactions  fact_payments  fact_credit_risk                │
│   agg_daily_balances  agg_monthly_summary  agg_branch_performance   │
│   agg_customer_360                                                  │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                    Redshift COPY commands
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  REDSHIFT SERVERLESS (Analytics Layer)               │
│   banking schema — 11 tables + 4 analytical views                   │
│   Connected to Apache Superset for dashboards                       │
└─────────────────────────────────────────────────────────────────────┘

ORCHESTRATION LAYER
┌─────────────────────────────────────────────────────────────────────┐
│  EventBridge Scheduler (daily 02:00 IST)                            │
│       → Step Functions State Machine                                │
│          → Parallel Silver (6 jobs) → Sequential Dims               │
│          → Parallel Facts (3 jobs) → Aggregations → Redshift COPY  │
│          → SNS Success/Failure notification                         │
└─────────────────────────────────────────────────────────────────────┘

MONITORING LAYER
┌─────────────────────────────────────────────────────────────────────┐
│  CloudWatch Dashboards (3) │ 12 Alarms │ Anomaly Detection          │
│  Custom Metrics Lambda     │ DQ metrics │ SNS email/SMS alerts       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Data Volume

| Source | Daily Volume | Format | Load Type |
|--------|-------------|--------|-----------|
| RDS: transactions | ~1.5 GB | Parquet | Incremental (watermark) |
| RDS: accounts | ~200 MB | Parquet | Incremental (updated_at) |
| RDS: customers | ~100 MB | Parquet | SCD Type 2 |
| RDS: branches | ~1 MB | Parquet | Full refresh |
| CSV: payment_gateway | ~4 GB | Parquet | Incremental (file-based) |
| CSV: credit_bureau | ~1 GB | Parquet | Incremental (file-based) |
| **Total** | **~10+ GB/day** | | |

---

## 🛠️ AWS Services Used

| Category | Service | Purpose |
|----------|---------|---------|
| Storage | **S3** | Data Lake (Bronze/Silver/Gold/Quarantine zones) |
| Compute | **AWS Glue** | Visual ETL, Data Quality, transformations (13 jobs) |
| Database | **RDS SQL Server** | Source OLTP system (Express, free tier) |
| Warehouse | **Redshift Serverless** | Analytics / reporting layer |
| Orchestration | **Step Functions** | DAG with retry, parallel execution, error handling |
| Scheduling | **EventBridge Scheduler** | Daily 02:00 IST cron trigger |
| Compute | **Lambda** | Custom metrics publisher |
| Monitoring | **CloudWatch** | Dashboards, alarms, anomaly detection, logs |
| Alerting | **SNS** | Email/SMS on success/failure |
| Governance | **Glue Data Catalog** | Schema registry for all silver/gold tables |
| Security | **IAM** | Least-privilege roles per service |
| BI | **Apache Superset** | Self-hosted dashboards on EC2 |
| Query | **Athena** | Ad-hoc SQL queries on S3 Parquet |

---

## 📁 Repository Structure

```
neobank-aws-pipeline/
│
├── README.md                          ← You are here
├── ARCHITECTURE.md                    ← Deep-dive architecture decisions
├── DATA_DICTIONARY.md                 ← All columns documented
├── RUNBOOK.md                         ← Day-to-day operations guide
│
├── config/
│   ├── pipeline_config.json           ← Central config (bucket, region, thresholds)
│   ├── watermark_template.json        ← Watermark schema template
│   └── requirements.txt              ← All Python dependencies
│
├── infrastructure/
│   ├── iam_policies/
│   │   ├── glue_role_policy.json
│   │   ├── stepfunctions_role_policy.json
│   │   ├── scheduler_trust_policy.json
│   │   └── metrics_lambda_policy.json
│   └── redshift/
│       └── workgroup_config.md
│
├── sql/
│   ├── 01_create_tables.sql           ← RDS schema DDL
│   ├── 02_insert_historical_data.sql  ← Seed data (500+ customers, 30K+ txns)
│   ├── 03_incremental_data.sql        ← Incremental data simulation
│   └── redshift_analytical_views.sql  ← 4 BI-ready views
│
├── phase2_s3_datalake/
│   ├── README.md
│   ├── step1_verify_aws_setup.py
│   ├── step2_create_s3_bucket.py
│   ├── step3_upload_csv_landing.py
│   ├── step4_convert_csv_to_parquet.py
│   ├── step5_upload_bronze_parquet.py
│   ├── step6_verify_s3_structure.py
│   └── step7_create_metadata_files.py
│
├── phase3_rds_ingestion/
│   ├── README.md
│   ├── step1_test_rds_connection.py
│   ├── step2_extract_rds_to_bronze.py  ← Incremental extraction with watermark
│   └── step3_verify_bronze_layer.py
│
├── phase4_silver_glue/
│   ├── README.md
│   ├── step1_create_glue_iam_role.py
│   ├── step2_run_glue_jobs.py
│   ├── step3_verify_silver_layer.py
│   └── glue_job_configs/
│       ├── bronze_to_silver_branches.md
│       ├── bronze_to_silver_customers.md   ← PII masking config
│       ├── bronze_to_silver_accounts.md
│       ├── bronze_to_silver_transactions.md
│       ├── bronze_to_silver_payment_gateway.md
│       └── bronze_to_silver_credit_bureau.md
│
├── phase5_gold_star_schema/
│   ├── README.md
│   ├── step1_create_dim_date.py        ← 2020-2030 date dimension
│   ├── step2_verify_gold_layer.py
│   └── glue_job_configs/
│       ├── silver_to_gold_dim_branch.md
│       ├── silver_to_gold_dim_customer.md  ← SCD Type 2
│       ├── silver_to_gold_dim_account.md
│       ├── silver_to_gold_fact_transactions.md
│       ├── silver_to_gold_fact_payments.md
│       ├── silver_to_gold_fact_credit_risk.md
│       └── silver_to_gold_aggregations.md
│
├── phase6_redshift/
│   ├── README.md
│   ├── step1_create_redshift_schema.py  ← 11 tables DDL
│   ├── step2_load_gold_to_redshift.py   ← COPY from S3 Parquet
│   └── step3_verify_redshift.py
│
├── phase7_orchestration/
│   ├── README.md
│   ├── step1_create_state_machine.py
│   ├── step2_test_manual_run.py
│   ├── step3_create_eventbridge_schedule.py
│   ├── step4_verify_orchestration.py
│   └── phase7_state_machine.asl.json    ← Step Functions ASL definition
│
├── phase8_monitoring/
│   ├── README.md
│   ├── step1_create_dashboards.py
│   ├── step2_create_alarms.py
│   ├── step3_deploy_metrics_lambda.py
│   ├── step4_setup_anomaly_detection.py
│   └── step5_verify_monitoring.py
│
└── tests/
    ├── test_bronze_schema.py
    ├── test_silver_dq.py
    └── test_gold_integrity.py
```

---

## 🚀 Quick Start

### Prerequisites

```bash
# Python 3.11+
python --version

# AWS CLI configured
aws configure
aws sts get-caller-identity   # should show your Account ID

# Clone repo
git clone https://github.com/YOUR_USERNAME/neobank-aws-pipeline.git
cd neobank-aws-pipeline

# Create virtualenv and install dependencies
python -m venv venv
source venv/bin/activate         # Windows: venv\Scripts\activate
pip install -r config/requirements.txt
```

### Execution Order

Run phases in order — each phase depends on the previous.

```bash
# PHASE 2 — S3 Data Lake
cd phase2_s3_datalake
python step1_verify_aws_setup.py
python step2_create_s3_bucket.py
python step3_upload_csv_landing.py
python step4_convert_csv_to_parquet.py
python step5_upload_bronze_parquet.py
python step6_verify_s3_structure.py
python step7_create_metadata_files.py

# PHASE 3 — RDS Extraction
cd ../phase3_rds_ingestion
python step1_test_rds_connection.py
python step2_extract_rds_to_bronze.py
python step3_verify_bronze_layer.py

# PHASE 4 — Silver Layer (run in AWS Console + trigger via script)
cd ../phase4_silver_glue
python step1_create_glue_iam_role.py
# → Create 6 Glue Visual ETL jobs in console (see glue_job_configs/)
python step2_run_glue_jobs.py
python step3_verify_silver_layer.py

# PHASE 5 — Gold Star Schema
cd ../phase5_gold_star_schema
python step1_create_dim_date.py
# → Create 7 Glue Visual ETL jobs in console (see glue_job_configs/)
python step2_verify_gold_layer.py

# PHASE 6 — Redshift
cd ../phase6_redshift
python step1_create_redshift_schema.py
python step2_load_gold_to_redshift.py
python step3_verify_redshift.py

# PHASE 7 — Orchestration
cd ../phase7_orchestration
# → Create IAM roles in console (see README.md)
python step1_create_state_machine.py
python step2_test_manual_run.py
python step3_create_eventbridge_schedule.py
python step4_verify_orchestration.py

# PHASE 8 — Monitoring
cd ../phase8_monitoring
python step1_create_dashboards.py
python step2_create_alarms.py
python step3_deploy_metrics_lambda.py
python step4_setup_anomaly_detection.py
python step5_verify_monitoring.py
```

---

## 🔐 Key Design Decisions

### 1. Incremental Loads with Watermarking
Every source table has a watermark entry in `metadata/watermarks/watermark.json`. The extraction script reads `last_successful_run` and issues `WHERE updated_at > :watermark`. After a successful run, the watermark is updated — guaranteeing no data is lost or double-loaded.

### 2. Data Quality Architecture
Each silver Glue job runs 7 DQ rules. Records that **pass** go to `silver/<table>/`. Records that **fail** go to `quarantine/<table>/` with a `dq_failure_reason` column. The `dq_status` column in silver is always `PASSED`. DQ results are written as JSON to `metadata/dq_results/` for the metrics Lambda to read.

### 3. PII Masking in Silver
Customer PII is masked in the silver layer:
- `pan_number` → `pan_masked` (format: `ABCDE****F`)
- `email` → `email_masked` (format: `a***@***.com`)
- `phone_number` → `phone_masked` (format: `+91 XXXXXX7890`)

Original columns are **dropped** in silver. The only source that retains this data is the bronze zone — access controlled via IAM.

### 4. SCD Type 2 for Dimensions
`dim_customer` and `dim_account` implement SCD Type 2:
- `is_current = 1` for the current record
- `effective_from` / `effective_to` date range
- New surrogate key assigned on each change
- Historical rows preserved with `is_current = 0`

### 5. Step Functions Over Airflow
Step Functions Standard Workflow was chosen for:
- Native `.waitForTaskToken` integration with Glue (no polling Lambda needed)
- Managed retry with exponential backoff + jitter
- Visual debugging in console
- Zero infrastructure to manage (vs MWAA at ~$300/month)

### 6. Redshift Serverless
No cluster to manage. Auto-scales RPUs from 8 to 512 based on query demand. For daily batch + BI queries, cost is typically **under $20/month** in ap-south-1.

---

## 📈 Pipeline Execution Timeline

```
02:00 IST — EventBridge triggers Step Functions
02:01     — Silver layer starts (6 jobs in parallel)
02:06     — Silver complete (~5 min, bottleneck: transactions 30K rows)
02:07     — dim_branch starts (sequential dim chain)
02:08     — dim_customer starts (depends on dim_branch)
02:09     — dim_account starts (depends on dim_customer)
02:10     — Gold facts start (3 jobs in parallel)
02:14     — Aggregations job starts
02:17     — Redshift COPY starts (11 tables)
02:19     — Redshift COPY complete
02:19     — SNS success email sent
         Total: ~19 minutes end-to-end
```

---

## 💰 Monthly Cost Estimate (ap-south-1)

| Service | Usage | Cost |
|---------|-------|------|
| S3 Storage (~50 GB) | All zones | ~$1.15 |
| S3 Requests | PUT/GET | ~$0.50 |
| Glue (13 jobs × 2 DPU × 20 min daily) | ~3 DPU-hr/day | ~$4.32 |
| Redshift Serverless (8 RPU, 19 min/day) | ~0.025 RPU-hr | ~$0.70 |
| Step Functions (1050 transitions/month) | Free tier | $0.00 |
| EventBridge Scheduler (30 invocations) | Free tier | $0.00 |
| Lambda (30 invocations × 5 min) | Free tier | $0.00 |
| CloudWatch Dashboards (3) | 3 dashboards | $9.00 |
| CloudWatch Alarms (12) | 12 alarms | $1.20 |
| SNS (60 emails/month) | Free tier | $0.00 |
| RDS SQL Server Express | db.t3.micro | $0.00 (free tier) |
| **Total** | | **~$17/month** |

> **Cost optimization tip**: Skip CloudWatch dashboards for a learning project → ~$8/month total.

---

## 📋 Data Model

### Silver Layer (6 tables)
| Table | Rows | Key Transformations |
|-------|------|---------------------|
| silver.branches | 5 | DQ checks, metadata added |
| silver.customers | 500+ | PII masked, 5 DQ rules |
| silver.accounts | 1000+ | 7 DQ rules, balance validation |
| silver.transactions | 30000+ | Amount validation, channel check |
| silver.payment_gateway_logs | 20000+ | Response code validation |
| silver.credit_bureau_reports | 5500+ | risk_band enrichment |

### Gold Layer (11 tables)
| Table | Type | Rows | Description |
|-------|------|------|-------------|
| dim_date | Dimension | 3,653 | 2020–2030 calendar + fiscal year |
| dim_branch | Dimension | 5 | SCD Type 2 branch attributes |
| dim_customer | Dimension | 500+ | SCD Type 2 customer attributes |
| dim_account | Dimension | 1000+ | SCD Type 2 account attributes |
| fact_transactions | Fact | 30000+ | All banking transactions |
| fact_payments | Fact | 20000+ | Payment gateway events |
| fact_credit_risk | Fact | 5500+ | Credit bureau assessments |
| agg_daily_balances | Aggregate | 1000+ | Daily balance changes per account |
| agg_monthly_summary | Aggregate | 100+ | Monthly volume per branch |
| agg_branch_performance | Aggregate | 5 | Lifetime performance per branch |
| agg_customer_360 | Aggregate | 500+ | Lifetime view per customer |

---

## 🔍 Monitoring & Alerting

### CloudWatch Dashboards
| Dashboard | Audience | Key Widgets |
|-----------|----------|-------------|
| NeoBank-Pipeline-Overview | Leadership | SLA tracker, daily success/fail, cost trend |
| NeoBank-Glue-Jobs | Operations | Per-job duration, DPU usage, failure rate |
| NeoBank-Data-Quality | DQ Engineers | Pass rate per table, quarantine trend |

### Alarm Matrix
| Alarm | Severity | Trigger |
|-------|----------|---------|
| Pipeline failed | 🔴 CRITICAL | Step Functions ExecutionsFailed ≥ 1 |
| Pipeline timeout | 🔴 CRITICAL | Execution > 30 minutes |
| Multiple Glue failures | 🔴 CRITICAL | 2+ job failures in 1 hour |
| Slow silver job | 🟡 WARNING | transactions job > 10 min |
| DQ pass rate low | 🟡 WARNING | Pass rate < 95% |
| Quarantine spike | 🟡 WARNING | >50 quarantined rows |
| Row count anomaly | 🟡 WARNING | 2+ std devs from baseline |
| Daily success | 🔵 INFO | ExecutionsSucceeded = 1 |

---

## 🧪 Running Tests

```bash
cd tests

# Test bronze schema (columns, row_hash format, metadata presence)
python test_bronze_schema.py

# Test silver DQ (no raw PII, dq_status = PASSED, no DQ columns in output)
python test_silver_dq.py

# Test gold integrity (PK nulls, FK join rate, min row counts)
python test_gold_integrity.py
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/add-kafka-ingestion`
3. Commit changes: `git commit -m 'feat: add Kinesis streaming source'`
4. Push and open a Pull Request

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 👤 Author

Built as a production-grade portfolio project demonstrating AWS Data Engineering skills across:
- Medallion Architecture
- Data Quality Engineering
- Dimensional Modeling (Star Schema)
- Cloud Data Warehousing
- Workflow Orchestration
- Production Monitoring & Alerting

---

*Region: ap-south-1 (Mumbai) | Bucket: neo-bank-datalake | Stack: AWS Native*
