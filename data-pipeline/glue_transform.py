"""
glue_transform.py
=================
AWS Glue PySpark ETL job that reads dirty order data from the raw S3 bucket,
applies a series of data-quality transformations, and writes clean Parquet to
the clean S3 bucket.

TRANSFORMATIONS APPLIED
-----------------------
1.  Remove exact duplicate rows
2.  Trim whitespace from all string columns
3.  Standardise customer_name / country  -> Title Case
4.  Normalise status                     -> lower-case
5.  Normalise email                      -> lower-case
6.  Replace literal string 'NULL' / 'null' with real null
7.  Parse mixed date formats into a standard yyyy-MM-dd string
8.  Drop rows where order_id, customer_name, or email is null/empty
9.  Drop rows where product is null / 'NULL'
10. Drop rows where unit_price is null or <= 0
11. Drop rows where quantity <= 0 or unrealistically large (> 10000)
12. Drop rows where country is null/empty
13. Validate email format (must contain '@' and a '.')
14. Cast quantity to integer, unit_price to double
15. Add derived column: order_total = quantity * unit_price
16. Add pipeline audit columns: cleaned_at (UTC timestamp), pipeline_version
"""

import sys
import re
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType


# ─── Job bootstrap ──────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "INPUT_PATH",
    "OUTPUT_PATH",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

INPUT_PATH       = args["INPUT_PATH"]
OUTPUT_PATH      = args["OUTPUT_PATH"]
PIPELINE_VERSION = "1.0.0"

print(f"[INFO] Job      : {args['JOB_NAME']}")
print(f"[INFO] Input    : {INPUT_PATH}")
print(f"[INFO] Output   : {OUTPUT_PATH}")


# ─── 1. Read raw CSV ─────────────────────────────────────────────────────────
print("\n[STEP 1] Reading raw CSV from S3...")
raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")   # keep everything as strings first
    .csv(INPUT_PATH)
)
raw_count = raw_df.count()
print(f"         Raw row count : {raw_count}")
raw_df.printSchema()


# ─── 2. Remove exact duplicate rows ─────────────────────────────────────────
print("\n[STEP 2] Removing duplicate rows...")
df = raw_df.dropDuplicates()
print(f"         After dedup   : {df.count()} rows  (removed {raw_count - df.count()})")


# ─── 3. Trim whitespace on all columns ──────────────────────────────────────
print("\n[STEP 3] Trimming whitespace...")
for col_name in df.columns:
    df = df.withColumn(col_name, F.trim(F.col(col_name)))


# ─── 4. Replace literal string 'NULL' with real null ────────────────────────
print("\n[STEP 4] Replacing literal 'NULL' strings with null values...")
for col_name in df.columns:
    df = df.withColumn(
        col_name,
        F.when(F.upper(F.col(col_name)) == "NULL", None)
         .otherwise(F.col(col_name))
    )


# ─── 5. Normalise text columns ────────────────────────────────────────────────
print("\n[STEP 5] Normalising text casing...")
df = (
    df
    .withColumn("customer_name", F.initcap(F.col("customer_name")))
    .withColumn("country",       F.initcap(F.col("country")))
    .withColumn("status",        F.lower(F.col("status")))
    .withColumn("email",         F.lower(F.col("email")))
)


# ─── 6. Normalise mixed date formats ─────────────────────────────────────────
# Supported inputs:  2024-01-15  |  15/01/2024  |  Jan 18 2024  |  2024/01/23
print("\n[STEP 6] Normalising date formats to yyyy-MM-dd...")

def parse_date(raw: str):
    if raw is None:
        return None
    raw = raw.strip()
    formats = [
        "%Y-%m-%d",   # 2024-01-15
        "%d/%m/%Y",   # 15/01/2024
        "%Y/%m/%d",   # 2024/01/23
        "%b %d %Y",   # Jan 18 2024
        "%B %d %Y",   # January 18 2024
        "%d-%m-%Y",   # 18-01-2024
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None   # unparseable -> will be dropped

parse_date_udf = F.udf(parse_date)
df = df.withColumn("order_date", parse_date_udf(F.col("order_date")))


# ─── 7. Drop rows that fail mandatory-field checks ────────────────────────────
print("\n[STEP 7] Dropping rows with missing mandatory fields...")

before = df.count()
df = df.filter(
    F.col("order_id").isNotNull() & (F.col("order_id") != "") &
    F.col("customer_name").isNotNull() & (F.col("customer_name") != "") &
    F.col("product").isNotNull() & (F.col("product") != "") &
    F.col("order_date").isNotNull() &
    F.col("country").isNotNull() & (F.col("country") != "")
)
print(f"         Dropped {before - df.count()} rows with missing mandatory fields")


# ─── 8. Validate email format ────────────────────────────────────────────────
print("\n[STEP 8] Validating email addresses...")
email_regex = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
before = df.count()
df = df.filter(
    F.col("email").isNull() |
    F.col("email").rlike(email_regex)
)
print(f"         Dropped {before - df.count()} rows with invalid email")


# ─── 9. Cast numeric columns and validate ranges ──────────────────────────────
print("\n[STEP 9] Casting and validating numeric columns...")
df = (
    df
    .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
)

before = df.count()
df = df.filter(
    F.col("quantity").isNotNull() &
    (F.col("quantity") > 0) &
    (F.col("quantity") <= 10000) &       # flag unrealistic bulk orders
    F.col("unit_price").isNotNull() &
    (F.col("unit_price") > 0.0)
)
print(f"         Dropped {before - df.count()} rows with invalid quantity or price")


# ─── 10. Derive order_total & add audit columns ───────────────────────────────
print("\n[STEP 10] Adding derived and audit columns...")
df = (
    df
    .withColumn("order_total",      F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("cleaned_at",       F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
    .withColumn("pipeline_version", F.lit(PIPELINE_VERSION))
)


# ─── 11. Final summary ────────────────────────────────────────────────────────
clean_count = df.count()
print(f"\n[SUMMARY] Raw rows    : {raw_count}")
print(f"[SUMMARY] Clean rows  : {clean_count}")
print(f"[SUMMARY] Removed     : {raw_count - clean_count}")
print("\n[PREVIEW] First 5 clean rows:")
df.show(5, truncate=False)
df.printSchema()


# ─── 12. Write clean data as Parquet (partitioned by status) ──────────────────
print(f"\n[STEP 12] Writing Parquet to {OUTPUT_PATH} ...")
(
    df
    .repartition(1)               # single file for small dataset; remove for large
    .write
    .mode("overwrite")
    .partitionBy("status")
    .parquet(OUTPUT_PATH)
)
print("         Write complete.")


# ─── Commit job ──────────────────────────────────────────────────────────────
job.commit()
print("\n[INFO] Job committed successfully.")
