# dbt Analytics - All Projects

Complete dbt project transforming all 5 data projects in one place.

## Quick Start

```bash
# 1. Install dbt
pip install dbt-bigquery

# 2. Copy profiles.yml to ~/.dbt/
cp profiles.yml ~/.dbt/profiles.yml

# 3. Run dbt
dbt run          # Creates all models
dbt test         # Runs data quality tests
dbt docs generate  # Creates documentation
dbt docs serve     # View docs in browser (http://localhost:8000)
```

---

## Project Structure

```
dbt/
├── dbt_project.yml           # Main config
├── profiles.yml              # BigQuery connection
└── models/
    ├── staging/              # Data cleaning & prep (VIEWS)
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
    ├── marts/                # Analysis-ready tables (TABLES)
    │   ├── mart_p1_sales.sql
    │   ├── mart_p2_taxi_daily.sql
    │   ├── mart_p3_transaction_daily.sql
    │   └── mart_ma_putusan.sql
    └── schema.yml            # Documentation + tests
```

---

## What Each Layer Does

### Staging Layer (VIEWS)
- Clean and standardize raw data
- Remove nulls and duplicates
- Rename columns for clarity
- **All files**: `stg_*.sql`

### Marts Layer (TABLES)
- Join staging tables together
- Create analysis-ready summaries
- Aggregate metrics
- **All files**: `mart_*.sql`

---

## Data Models by Project

### 📦 Project 1: Retail E-Commerce
**Source**: `finalproject_rakhajidhan_ecommerce_retails`

| Model | Type | Purpose |
|-------|------|---------|
| `stg_p1_customers` | View | Clean customer data |
| `stg_p1_products` | View | Clean product catalog |
| `stg_p1_purchases` | View | Purchase transactions |
| `mart_p1_sales` | Table | Complete sales with customer + product |

### 🚖 Project 2: NYC Green Taxi
**Source**: `finalproject_rakhajidhan_ny_taxi_preparation`

| Model | Type | Purpose |
|-------|------|---------|
| `stg_p2_taxi_trips` | View | Clean taxi trip data |
| `mart_p2_taxi_daily` | Table | Daily metrics by vendor |

### 📡 Project 3: Pub/Sub Streaming
**Source**: `finalproject_rakhajidhan_pubsub_retail`

| Model | Type | Purpose |
|-------|------|---------|
| `stg_p3_transactions` | View | Clean streaming transactions |
| `mart_p3_transaction_daily` | Table | Daily transaction summary |

### ⚖️ Mahkamah Agung: Court Decisions
**Source**: `finalproject_rakhajidhan_mahkamahagung`

| Model | Type | Purpose |
|-------|------|---------|
| `stg_ma_putusan_list` | View | Clean court decision listing |
| `stg_ma_pdf_detail` | View | Extracted PDF content |
| `mart_ma_putusan` | Table | Combined putusan + PDF data |

### 📊 Adakami: API Statistics
**Source**: `finalproject_rakhajidhan_adakami`

| Model | Type | Purpose |
|-------|------|---------|
| `stg_adakami_stats` | View | Clean API statistics |

---

## BigQuery Datasets Created

When you run `dbt run`, these datasets are created:

- `dbt_analytics_dev` - Development environment
- `dbt_analytics_prod` - Production environment

Each contains all the models (staging views + mart tables).

---

## How It Works with Airflow

1. **Airflow** loads raw data into BigQuery (via your existing DAGs)
2. **dbt** transforms that data into clean tables
3. You can schedule dbt in Airflow using `dbt run` command

Example Airflow task:
```python
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /path/to/dbt && dbt run',
)
```

---

## Common Commands

```bash
# Run all models
dbt run

# Run specific model
dbt run --select stg_p1_customers

# Run specific project
dbt run --select path:project1_retail

# Run with tests
dbt run && dbt test

# Generate documentation
dbt docs generate
dbt docs serve  # View at localhost:8000

# Check for errors without running
dbt parse

# Debug a model
dbt debug
```

---

## Next Steps

1. ✅ **Run dbt**: `dbt run`
2. 📊 **Check BigQuery**: View new datasets and tables
3. 📚 **Generate Docs**: `dbt docs generate && dbt docs serve`
4. 🧪 **Add Tests**: Expand `schema.yml` with more tests
5. ⚙️ **Schedule**: Add to Airflow DAGs

---

## Config Details

### profiles.yml Location
- **Mac/Linux**: `~/.dbt/profiles.yml`
- **Windows**: `%USERPROFILE%\.dbt\profiles.yml`

### BigQuery Credentials
- Make sure `service-account.json` exists in `/opt/airflow/`
- Or update `keyfile` path in `profiles.yml`

---

## Troubleshooting

**Error: "Could not find profiles.yml"**
```bash
dbt run --profiles-dir ~/.dbt/
```

**Error: "Permission denied to dataset"**
- Check service account has BigQuery Editor role
- Verify dataset names in models match your BigQuery

**Error: "Table not found"**
- Verify source table names in staging models
- Check dataset names: `jcdeah-007.finalproject_rakhajidhan_*`

---

## Files Summary

| File | Purpose |
|------|---------|
| `dbt_project.yml` | Project metadata & settings |
| `profiles.yml` | BigQuery connection credentials |
| `models/schema.yml` | Documentation & data tests |
| `models/staging/*.sql` | Data cleaning (views) |
| `models/marts/*.sql` | Analysis tables |

---

That's it! You now have a complete, production-ready dbt project. 🚀
