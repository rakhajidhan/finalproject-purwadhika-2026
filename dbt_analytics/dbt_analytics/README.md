# dbt Analytics — Final Project

Complete dbt transformation project for all 5 data pipelines.

## Setup

```bash
# 1. Install dbt with BigQuery adapter
pip install dbt-bigquery

# 2. Copy profiles.yml to dbt home
cp profiles.yml ~/.dbt/profiles.yml

# 3. Verify connection
dbt debug

# 4. Run all models
dbt run

# 5. Run tests
dbt test

# 6. Generate and view docs
dbt docs generate
dbt docs serve   # Opens at http://localhost:8000
```

---

## Project Structure

```
dbt_analytics/
├── dbt_project.yml
├── profiles.yml              ← BigQuery connection (copy to ~/.dbt/)
├── README.md
└── models/
    ├── schema.yml            ← All sources, models, tests & descriptions
    ├── staging/
    │   ├── project1_retail/
    │   │   ├── stg_p1_customers.sql
    │   │   ├── stg_p1_products.sql
    │   │   └── stg_p1_purchases.sql
    │   ├── project2_taxi/
    │   │   └── stg_p2_taxi_trips.sql
    │   ├── project3_pubsub/
    │   │   └── stg_p3_transactions.sql
    │   ├── mahkamah_agung/
    │   │   ├── stg_ma_putusan_list.sql
    │   │   └── stg_ma_pdf_detail.sql
    │   └── adakami/
    │       └── stg_adakami_stats.sql
    └── marts/
        ├── mart_p1_sales.sql
        ├── mart_p2_taxi_daily.sql
        ├── mart_p3_transaction_daily.sql
        └── mart_ma_putusan.sql
```

---

## Data Flow

```
BigQuery Raw Tables (loaded by Airflow)
          │
          ▼
  Staging Layer (VIEWs)      ← Clean, filter, rename columns
          │
          ▼
   Mart Layer (TABLEs)        ← Join, aggregate, analysis-ready
```

### Staging Models (Views)
| Model | Source Dataset | Purpose |
|---|---|---|
| `stg_p1_customers` | ecommerce_retails | Customer master |
| `stg_p1_products` | ecommerce_retails | Product catalog |
| `stg_p1_purchases` | ecommerce_retails | Purchase transactions |
| `stg_p2_taxi_trips` | ny_taxi_preparation | Green taxi trips |
| `stg_p3_transactions` | pubsub_retail | Streaming transactions |
| `stg_ma_putusan_list` | mahkamahagung | Court decision listing |
| `stg_ma_pdf_detail` | mahkamahagung | PDF text extraction |
| `stg_adakami_stats` | adakami | API statistics |

### Mart Models (Tables)
| Model | Joins | Purpose |
|---|---|---|
| `mart_p1_sales` | purchases + customers + products | Full sales detail |
| `mart_p2_taxi_daily` | taxi trips (aggregated) | Daily metrics by vendor |
| `mart_p3_transaction_daily` | transactions (aggregated) | Daily summary by customer/category |
| `mart_ma_putusan` | putusan_list + pdf_detail | Full court decision |

---

## BigQuery Output Datasets

| Environment | Dataset |
|---|---|
| Development | `jcdeah-007.dbt_analytics_dev` |
| Production | `jcdeah-007.dbt_analytics_prod` |

---

## Integrating with Airflow

Add a dbt task at the end of each pipeline DAG:

```python
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt_analytics && dbt run --profiles-dir ~/.dbt/',
)

# Chain after your load tasks:
load_task >> dbt_run
```

---

## Useful Commands

```bash
# Run a single model
dbt run --select mart_p1_sales

# Run all staging models
dbt run --select path:models/staging

# Run with full refresh (recreate tables)
dbt run --full-refresh

# Run only models that changed
dbt run --select state:modified

# Test a specific model
dbt test --select stg_p1_customers
```
