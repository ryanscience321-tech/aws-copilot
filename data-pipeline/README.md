# AWS Data Pipeline — Step-by-Step Guide

This pipeline ingests a CSV file with real-world data quality problems, discovers the schema with a Glue Crawler, cleanses the data with a Glue PySpark job, and writes clean Parquet files to a separate S3 bucket.

```
┌─────────────────┐     ┌───────────────────┐     ┌──────────────────────┐     ┌──────────────────┐
│  dirty CSV file │────▶│  S3 Raw Bucket    │────▶│  Glue Crawler        │────▶│  Glue Data       │
│  (local)        │     │  /raw/orders/     │     │  (infers schema)     │     │  Catalog Table   │
└─────────────────┘     └───────────────────┘     └──────────────────────┘     └──────────────────┘
                                │
                                ▼
                        ┌───────────────────┐     ┌──────────────────────┐
                        │  Glue ETL Job     │────▶│  S3 Clean Bucket     │
                        │  (PySpark)        │     │  /clean/orders/      │
                        │  glue_transform.py│     │  (Parquet, partd.)   │
                        └───────────────────┘     └──────────────────────┘
                                                           │
                                                           ▼
                                                  ┌──────────────────────┐
                                                  │  Amazon Athena       │
                                                  │  (SQL queries)       │
                                                  └──────────────────────┘
```

---

## Pre-requisites

| Requirement | Check |
|---|---|
| AWS CLI installed | `aws --version` |
| AWS CLI configured | `aws sts get-caller-identity` |
| PowerShell 5.1+ | `$PSVersionTable.PSVersion` |
| IAM user has permissions for S3, Glue, IAM, Athena | |

---

## File Map

| File | Purpose |
|---|---|
| `sample_orders_dirty.csv` | Input CSV with 10 types of data quality issues |
| `step1_create_buckets.ps1` | Creates 3 S3 buckets + saves `pipeline_config.json` |
| `step2_upload_data.ps1` | Uploads the dirty CSV to the raw bucket |
| `step3_create_glue_role.ps1` | Creates IAM role + policies for Glue |
| `step4_create_crawler.ps1` | Creates & runs Glue Crawler to discover schema |
| `glue_transform.py` | PySpark ETL script (uploaded to S3 by step 5) |
| `step5_create_etl_job.ps1` | Creates & runs the Glue ETL job |
| `step6_query_with_athena.ps1` | (Optional) Registers clean table + runs Athena queries |
| `cleanup_pipeline.ps1` | Tears down **all** resources created by these scripts |

---

## Run Order

Open PowerShell, `cd` into the `data-pipeline\` folder, then run each step in order:

### Step 1 — Create S3 Buckets

```powershell
cd "C:\Users\ryans\OneDrive\Desktop\Vibe coding mark 2\data-pipeline"
.\step1_create_buckets.ps1
```

**What happens:** Three S3 buckets are created and saved to `pipeline_config.json`:
- `data-pipeline-raw-<suffix>` — dirty files land here
- `data-pipeline-clean-<suffix>` — cleansed Parquet files go here
- `data-pipeline-scripts-<suffix>` — Glue script + temp storage

---

### Step 2 — Upload the Dirty Data File

```powershell
.\step2_upload_data.ps1
```

**What happens:** `sample_orders_dirty.csv` is copied to `s3://<raw>/raw/orders/`.  
The script also prints a summary of all 10 data quality issues embedded in the file:

| # | Problem | Example |
|---|---|---|
| 1 | Duplicate rows | order_id 1001 appears twice |
| 2 | Missing customer name / email | Row 1004 has no name |
| 3 | Mixed date formats | `2024-01-15` vs `15/01/2024` vs `Jan 18 2024` |
| 4 | Inconsistent casing | `usa` vs `USA`, `COMPLETED` vs `completed` |
| 5 | Negative quantity | `-1` on row 1006 |
| 6 | Zero quantity | `0` on row 1008 |
| 7 | Unrealistic quantity | `999999` on row 1017 |
| 8 | Literal string 'NULL' | product = `NULL` on row 1014 |
| 9 | Missing unit_price | blank on row 1020 |
| 10 | Invalid email | `jack@` (no domain) |

---

### Step 3 — Create IAM Role for Glue

```powershell
.\step3_create_glue_role.ps1
```

**What happens:** An IAM role named `GlueDataPipelineRole` is created with:
- `AWSGlueServiceRole` managed policy (CloudWatch logs + basic Glue)
- Inline policy scoped to only the 3 pipeline buckets

---

### Step 4 — Run the Glue Crawler

```powershell
.\step4_create_crawler.ps1
```

**What happens:**
1. A Glue Data Catalog database `data_pipeline_db` is created
2. A crawler `orders-raw-crawler` is created pointing at `s3://<raw>/raw/orders/`
3. The crawler runs (~1-2 minutes) and infers the table schema from the CSV
4. A table `raw_sample_orders_dirty` appears in the catalog

**View in Console:** AWS Console → Glue → Data Catalog → Tables → `raw_sample_orders_dirty`  
You will see every column Glue inferred, including the dirty data.

---

### Step 5 — Run the Glue ETL Cleanse Job

```powershell
.\step5_create_etl_job.ps1
```

**What happens:**
1. `glue_transform.py` is uploaded to the scripts bucket
2. A Glue Spark job `orders-data-cleanse-job` is created (Glue 4.0, Python 3, 2 x G.1X workers)
3. The job runs (~3-5 minutes) with `--INPUT_PATH` and `--OUTPUT_PATH` arguments
4. The script applies all 15 transformation rules (see below)
5. Clean Parquet files land in `s3://<clean>/clean/orders/` partitioned by `status`

**Transformations in `glue_transform.py`:**

| # | Rule |
|---|---|
| 1 | Remove exact duplicate rows |
| 2 | Trim leading/trailing whitespace from all strings |
| 3 | Standardise `customer_name` and `country` → Title Case |
| 4 | Normalise `status` → lower-case |
| 5 | Normalise `email` → lower-case |
| 6 | Replace literal `"NULL"` strings with real nulls |
| 7 | Parse all 4 date formats → `yyyy-MM-dd` |
| 8 | Drop rows with null `order_id`, `customer_name`, or `product` |
| 9 | Validate email format (regex) |
| 10 | Cast `quantity` → Integer, `unit_price` → Double |
| 11 | Drop rows where `quantity ≤ 0` or `quantity > 10000` |
| 12 | Drop rows where `unit_price ≤ 0` or null |
| 13 | Drop rows where `country` is null/empty |
| 14 | Add `order_total = quantity × unit_price` |
| 15 | Add `cleaned_at` timestamp + `pipeline_version` audit columns |

**View in Console:** AWS Console → Glue → Jobs → `orders-data-cleanse-job` → Run details  
You can also check CloudWatch Logs for the full PySpark output.

---

### Step 6 — Query with Athena (Optional)

```powershell
.\step6_query_with_athena.ps1
```

**What happens:** Runs a second crawler on the clean bucket, then executes three SQL queries via Athena to validate the results.

---

## Cleanup

When you're done, delete **all** resources (buckets, Glue jobs, IAM role) with:

```powershell
.\cleanup_pipeline.ps1
```

---

## Cost Estimate

| Resource | Approx. Cost |
|---|---|
| 3 × S3 buckets (tiny files) | < $0.01 |
| Glue Crawler (1-2 min) | ~$0.01 |
| Glue ETL Job (3-5 min, 2 DPU) | ~$0.07 |
| Athena queries (KB of data) | < $0.01 |
| **Total** | **< $0.10** |
