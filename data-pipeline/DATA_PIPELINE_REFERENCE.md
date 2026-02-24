# AWS Data Pipeline — Reference Guide

**Project:** Order Data Quality Pipeline  
**AWS Account:** 830087179367  
**Region:** us-east-1  
**Last Run:** February 2026  
**Total Cost:** < $0.10 per full run  

---

## Architecture Overview

```
Local CSV (dirty data)
        |
        v
S3 Raw Bucket  ──────────>  Glue Crawler  ──>  Glue Data Catalog
(input)                     (schema discovery)   (queryable table)
        |
        v
Glue ETL Job (PySpark)
(15 transformation rules)
        |
        v
S3 Clean Bucket
(Parquet, partitioned by status)
        |
        v
Amazon Athena (optional SQL queries)
```

---

## AWS Resources Created

| Resource | Name | Purpose |
|---|---|---|
| S3 Bucket (raw) | `data-pipeline-raw-<suffix>` | Stores the original dirty input files |
| S3 Bucket (clean) | `data-pipeline-clean-<suffix>` | Stores cleansed Parquet output |
| S3 Bucket (scripts) | `data-pipeline-scripts-<suffix>` | Stores Glue PySpark script + temp files |
| Glue Database | `data_pipeline_db` | Logical container for catalog tables |
| Glue Crawler | `orders-raw-crawler` | Infers schema from S3 CSV, creates catalog table |
| Glue ETL Job | `orders-data-cleanse-job` | PySpark job that runs the transformations |
| IAM Role | `GlueDataPipelineRole` | Role assumed by Glue — scoped S3 + CloudWatch access |

> **Suffix** = last 6 digits of your AWS Account ID (e.g. `179367` for account `830087179367`)

---

## File Inventory

```
data-pipeline/
├── sample_orders_dirty.csv     Input: 23 rows with 10 quality issues (see below)
├── glue_transform.py           PySpark ETL script — 15 transformation rules
├── step1_create_buckets.ps1    PowerShell: create 3 S3 buckets
├── step2_upload_data.ps1       PowerShell: upload CSV to raw bucket
├── step3_create_glue_role.ps1  PowerShell: create IAM role + policies
├── step4_create_crawler.ps1    PowerShell: create + run Glue Crawler
├── step5_create_etl_job.ps1    PowerShell: create + run Glue ETL Job
├── step6_query_with_athena.ps1 PowerShell: register clean table + Athena queries
├── cleanup_pipeline.ps1        PowerShell: delete all resources
└── DATA_PIPELINE_REFERENCE.md  This file
```

---

## How to Re-run the Pipeline

> **Pre-requisites:** AWS CLI configured (`aws sts get-caller-identity`), PowerShell 5.1+

```powershell
cd "C:\Users\ryans\OneDrive\Desktop\Vibe coding mark 2\data-pipeline"

.\step1_create_buckets.ps1      # ~30 sec  — creates 3 S3 buckets
.\step2_upload_data.ps1         # ~10 sec  — uploads dirty CSV
.\step3_create_glue_role.ps1    # ~20 sec  — creates IAM role
.\step4_create_crawler.ps1      # ~2 min   — discovers schema
.\step5_create_etl_job.ps1      # ~5 min   — cleanses data, writes Parquet
.\step6_query_with_athena.ps1   # optional — SQL queries on clean data
```

### Known Issue — Script Encoding
The `.ps1` files contain Unicode characters that cause PowerShell 5.1 to fail parsing.
**Workaround:** Run each step's commands inline in the terminal instead, or open each script, copy the body into a fresh `.ps1` file saved as **UTF-8 without BOM**.

---

## IAM Policies Applied to GlueDataPipelineRole

### Trust Policy (who can assume this role)
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "glue.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```

### Managed Policy Attached
- `arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole`
  - Covers: CloudWatch Logs, basic Glue Data Catalog, S3 default Glue paths

### Inline Policy — GluePipelineS3Access (scoped to our 3 buckets only)
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::data-pipeline-raw-<suffix>",
      "arn:aws:s3:::data-pipeline-raw-<suffix>/*",
      "arn:aws:s3:::data-pipeline-clean-<suffix>",
      "arn:aws:s3:::data-pipeline-clean-<suffix>/*",
      "arn:aws:s3:::data-pipeline-scripts-<suffix>",
      "arn:aws:s3:::data-pipeline-scripts-<suffix>/*"
    ]
  }]
}
```

---

## Glue Crawler Settings

| Setting | Value |
|---|---|
| Name | `orders-raw-crawler` |
| Target S3 path | `s3://data-pipeline-raw-<suffix>/raw/orders/` |
| Database | `data_pipeline_db` |
| Table prefix | `raw_` → table created as `raw_orders` |
| Schema change policy | UPDATE_IN_DATABASE / LOG |
| Typical run time | 1–2 minutes |

---

## Glue ETL Job Settings

| Setting | Value |
|---|---|
| Name | `orders-data-cleanse-job` |
| Glue version | 4.0 (PySpark, Python 3.10) |
| Worker type | G.1X |
| Workers | 2 |
| Timeout | 30 minutes |
| Script | `s3://data-pipeline-scripts-<suffix>/scripts/glue_transform.py` |
| Input arg `--INPUT_PATH` | `s3://data-pipeline-raw-<suffix>/raw/orders/` |
| Output arg `--OUTPUT_PATH` | `s3://data-pipeline-clean-<suffix>/clean/orders/` |
| Output format | Parquet (Snappy compressed) |
| Output partitioning | `status` column |
| Typical run time | ~70 seconds (for this small dataset) |

---

## Data Quality Issues in the Sample File

| # | Issue Type | Example |
|---|---|---|
| 1 | Duplicate rows | `order_id` 1001, 1003, 1016 each appear twice |
| 2 | Missing customer name | Row 1004 — blank `customer_name` |
| 3 | Missing email | Row 1011 — blank `email` |
| 4 | Missing product | Row 1007 — blank `product` |
| 5 | Mixed date formats | `2024-01-15` vs `15/01/2024` vs `Jan 18 2024` vs `2024/01/23` |
| 6 | Inconsistent casing | `usa` vs `USA`, `COMPLETED` vs `completed` |
| 7 | Negative quantity | Row 1006 — `quantity = -1` |
| 8 | Zero quantity | Row 1008 — `quantity = 0` |
| 9 | Unrealistic quantity | Row 1017 — `quantity = 999999` |
| 10 | Literal string NULL | Row 1014 — `product = "NULL"` |
| 11 | Missing unit_price | Row 1020 — blank price |
| 12 | Whitespace in name | Row 1016 — `"  Peter White  "` |
| 13 | Invalid email | Row 1010 — `jack@` (no domain) |

---

## Transformation Rules in glue_transform.py

| Step | Rule |
|---|---|
| 1 | `dropDuplicates()` — remove exact duplicate rows |
| 2 | `trim()` all string columns — remove leading/trailing whitespace |
| 3 | Replace literal `"NULL"` / `"null"` strings with real null values |
| 4 | `initcap()` on `customer_name` and `country` → Title Case |
| 5 | `lower()` on `status` and `email` → lower-case |
| 6 | Parse 4 date formats to standard `yyyy-MM-dd` via Python UDF |
| 7 | Drop rows where `order_id`, `customer_name`, `product`, or `order_date` is null |
| 8 | Drop rows where `country` is null or empty |
| 9 | Validate email with regex — drop rows with malformed addresses |
| 10 | Cast `quantity` → `IntegerType`, `unit_price` → `DoubleType` |
| 11 | Drop rows where `quantity <= 0` or `quantity > 10000` |
| 12 | Drop rows where `unit_price <= 0` or null |
| 13 | Add derived column: `order_total = ROUND(quantity * unit_price, 2)` |
| 14 | Add audit column: `cleaned_at` — UTC timestamp of when job ran |
| 15 | Add audit column: `pipeline_version = "1.0.0"` |

---

## Output Schema (after cleaning)

| Column | Type | Notes |
|---|---|---|
| `order_id` | string | Mandatory, deduplicated |
| `customer_name` | string | Title Case, no nulls |
| `email` | string | lower-case, regex-validated |
| `order_date` | string | Standardised to `yyyy-MM-dd` |
| `product` | string | No nulls, no literal "NULL" |
| `quantity` | integer | 1–10000 range enforced |
| `unit_price` | double | Must be > 0 |
| `country` | string | Title Case, no nulls |
| `status` | string | lower-case (partition key) |
| `order_total` | double | Derived: `quantity * unit_price` |
| `cleaned_at` | string | UTC timestamp added by pipeline |
| `pipeline_version` | string | `"1.0.0"` |

---

## Output File Locations

```
s3://data-pipeline-clean-<suffix>/clean/orders/
    status=completed/   part-00000-....snappy.parquet
    status=pending/     part-00000-....snappy.parquet
    status=shipped/     part-00000-....snappy.parquet
```

---

## Querying with Athena

After running `step6_query_with_athena.ps1`, a second crawler (`orders-clean-crawler`) registers
the clean Parquet as table `clean_orders` in the `data_pipeline_db` catalog.

Open **AWS Console → Athena → Query Editor**, select database `data_pipeline_db`, and run:

```sql
-- Count of clean orders
SELECT COUNT(*) FROM clean_orders;

-- Revenue by status
SELECT status, COUNT(*) AS orders, ROUND(SUM(order_total),2) AS revenue
FROM clean_orders
GROUP BY status ORDER BY revenue DESC;

-- Orders by country
SELECT country, COUNT(*) AS orders
FROM clean_orders
GROUP BY country ORDER BY orders DESC;

-- Find a specific customer
SELECT * FROM clean_orders WHERE customer_name = 'Alice Johnson';
```

---

## Cleanup — Remove All Resources

```powershell
cd "C:\Users\ryans\OneDrive\Desktop\Vibe coding mark 2\data-pipeline"

# Empty and delete S3 buckets
foreach ($b in @("data-pipeline-raw-179367","data-pipeline-clean-179367","data-pipeline-scripts-179367")) {
    aws s3 rm "s3://$b" --recursive --region us-east-1 --no-cli-pager
    aws s3api delete-bucket --bucket $b --region us-east-1 --no-cli-pager
}

# Delete Glue resources
aws glue delete-crawler --name "orders-raw-crawler"   --region us-east-1 --no-cli-pager
aws glue delete-crawler --name "orders-clean-crawler" --region us-east-1 --no-cli-pager
aws glue delete-job --job-name "orders-data-cleanse-job" --region us-east-1 --no-cli-pager
aws glue delete-database --name "data_pipeline_db"    --region us-east-1 --no-cli-pager

# Delete IAM role
aws iam detach-role-policy --role-name "GlueDataPipelineRole" --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
aws iam delete-role-policy --role-name "GlueDataPipelineRole" --policy-name "GluePipelineS3Access"
aws iam delete-role --role-name "GlueDataPipelineRole"
```

---

## Cost Breakdown

| Resource | Billing Basis | Approx. Cost |
|---|---|---|
| S3 Storage (3 buckets, tiny files) | per GB/month | < $0.01 |
| Glue Crawler (1-2 min) | $0.44 / DPU-hour | ~$0.01 |
| Glue ETL Job (~70 sec, 2x G.1X) | $0.44 / DPU-hour | ~$0.02 |
| Athena queries (KB of data scanned) | $5 / TB scanned | < $0.01 |
| **Total per run** | | **< $0.05** |

---

## Extending the Pipeline

**Add a new data source:** Create a new S3 prefix under `raw/`, create a dedicated crawler, and add a new Glue job or add a branch in `glue_transform.py`.

**Schedule it to run automatically:** In the AWS Console → Glue → Crawlers → Edit → Schedule (cron expression). For the job: Glue → Jobs → Schedules tab.

**Add more transformations:** Edit `glue_transform.py` and re-upload to S3, then re-run the job. The existing job definition automatically picks up the new script.

**Scale up for larger data:** Change `--number-of-workers` and `--worker-type` in the job definition. Remove `.repartition(1)` in the write step to produce multiple parallel Parquet files.

**Connect to BI tools:** Use Amazon QuickSight → New dataset → Athena → select `data_pipeline_db` → `clean_orders` to build dashboards directly on the clean data.
