"""
P005 - Event Driven Batch ETL Pipeline
Glue ETL script: deterministic quality checks + transform

Design principle:
  Glue produces evidence. Lambda decides batch status.
  Glue only raises exceptions for true technical failures, not quality thresholds.

Flow:
  1. Read raw CSV from S3 landing zone
  2. Cast and type-check all columns
  3. Classify each row: INVALID or VALID (warnings are a subset of valid)
  4. Write VALID rows (including warning rows) to curated Parquet
  5. Write INVALID rows to quarantine with reason codes + diagnostic columns
  6. Write quality report JSON to S3 for Lambda evaluator to make batch decision

Field severity:
  CRITICAL — invalidates the row:
    transactionid   : null or empty
    transactionts   : unparseable timestamp
    amount          : null, non-numeric, or <= 0
    quantity        : null, non-numeric, or <= 0

  WARNING — row is kept in curated, flagged in report:
    storeid         : null or empty
    productid       : null or empty

  ANOMALY — row is kept, surfaced as metrics for Lambda/agent to interpret:
    amount > 5000   : unusually large transaction
    quantity > 50   : unusually large quantity

Job parameters:
  --S3_KEY  : full S3 key, e.g. landing/transactions/dt=2026-03-18/transactions.csv
  --BUCKET  : S3 bucket name, e.g. nadav-p005-pipeline
"""

import sys
import re
import json
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import boto3

# ── Parameters ───────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_KEY", "BUCKET"])

S3_KEY = args["S3_KEY"]
BUCKET = args["BUCKET"]

match = re.search(r"dt=(\d{4}-\d{2}-\d{2})", S3_KEY)
if not match:
    raise Exception(f"Could not extract date from S3 key: {S3_KEY}")
SOURCE_DATE = match.group(1)

LANDING_PATH    = f"s3://{BUCKET}/landing/transactions/dt={SOURCE_DATE}/transactions.csv"
CURATED_PATH    = f"s3://{BUCKET}/curated/transactions/"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/transactions/dt={SOURCE_DATE}/"
REPORT_KEY      = f"quality-reports/dt={SOURCE_DATE}/report.json"

# ── Thresholds ────────────────────────────────────────────────────────────────
# Technical failure thresholds — Glue raises exception
MIN_ROW_COUNT = 50_000

# Quality thresholds — written to report, Lambda makes the batch decision
MAX_INVALID_RATE  = 0.05   # Lambda should ESCALATE or QUARANTINE above this
MAX_WARNING_RATE  = 0.10   # Lambda should ALLOW_WITH_WARNING above this

# Anomaly thresholds — rows kept, flagged as suspicious metrics in report
AMOUNT_ANOMALY_THRESHOLD   = 5_000.0
QUANTITY_ANOMALY_THRESHOLD = 50

# Hard invalidity rules — these are defensible business rules
AMOUNT_MIN   = 0.01   # amount must be positive
QUANTITY_MIN = 1      # quantity must be at least 1

# ── Init ──────────────────────────────────────────────────────────────────────
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Processing date : {SOURCE_DATE}")
print(f"Source          : {LANDING_PATH}")

# ── Schema ────────────────────────────────────────────────────────────────────
raw_schema = StructType([
    StructField("transactionid", StringType(), nullable=True),
    StructField("transactionts",  StringType(), nullable=True),
    StructField("storeid",        StringType(), nullable=True),
    StructField("productid",      StringType(), nullable=True),
    StructField("amount",         StringType(), nullable=True),
    StructField("quantity",       StringType(), nullable=True),
])

# ── Read ──────────────────────────────────────────────────────────────────────
print("Reading CSV...")
df = spark.read.option("header", "true").schema(raw_schema).csv(LANDING_PATH)
raw_count = df.count()
print(f"Raw row count: {raw_count:,}")

# Technical failure — abort if file is essentially empty
if raw_count < MIN_ROW_COUNT:
    raise Exception(f"Row count {raw_count} below minimum {MIN_ROW_COUNT}. Aborting.")

# ── Cast columns ──────────────────────────────────────────────────────────────
df = df \
    .withColumn("transactionts_parsed",
                F.to_timestamp("transactionts", "yyyy-MM-dd'T'HH:mm:ss")) \
    .withColumn("amount_parsed",   F.col("amount").cast(DoubleType())) \
    .withColumn("quantity_parsed", F.col("quantity").cast(IntegerType())) \
    .withColumn("dt", F.lit(SOURCE_DATE))

# ── Apply critical reason codes ───────────────────────────────────────────────
# _reasons: non-empty = row is INVALID and will be quarantined
df = df \
    .withColumn("_reasons", F.array()) \
    \
    .withColumn("_reasons", F.when(
        F.col("transactionid").isNull() | (F.trim(F.col("transactionid")) == ""),
        F.array_union(F.col("_reasons"), F.array(F.lit("missing_transactionid")))
    ).otherwise(F.col("_reasons"))) \
    \
    .withColumn("_reasons", F.when(
        F.col("transactionts_parsed").isNull(),
        F.array_union(F.col("_reasons"), F.array(F.lit("invalid_timestamp")))
    ).otherwise(F.col("_reasons"))) \
    \
    .withColumn("_reasons", F.when(
        F.col("amount_parsed").isNull(),
        F.array_union(F.col("_reasons"), F.array(F.lit("missing_amount")))
    ).when(
        F.col("amount_parsed") < AMOUNT_MIN,
        F.array_union(F.col("_reasons"), F.array(F.lit("invalid_amount")))
    ).otherwise(F.col("_reasons"))) \
    \
    .withColumn("_reasons", F.when(
        F.col("quantity_parsed").isNull(),
        F.array_union(F.col("_reasons"), F.array(F.lit("missing_quantity")))
    ).when(
        F.col("quantity_parsed") < QUANTITY_MIN,
        F.array_union(F.col("_reasons"), F.array(F.lit("invalid_quantity")))
    ).otherwise(F.col("_reasons")))

# ── Apply warning codes ───────────────────────────────────────────────────────
# _warn_reasons: non-empty = row is valid but flagged
# Warning rows ARE written to curated — they are not rejected
df = df \
    .withColumn("_warn_reasons", F.array()) \
    \
    .withColumn("_warn_reasons", F.when(
        F.col("storeid").isNull() | (F.trim(F.col("storeid")) == ""),
        F.array_union(F.col("_warn_reasons"), F.array(F.lit("missing_storeid")))
    ).otherwise(F.col("_warn_reasons"))) \
    \
    .withColumn("_warn_reasons", F.when(
        F.col("productid").isNull() | (F.trim(F.col("productid")) == ""),
        F.array_union(F.col("_warn_reasons"), F.array(F.lit("missing_productid")))
    ).otherwise(F.col("_warn_reasons")))

# ── Classify rows ─────────────────────────────────────────────────────────────
invalid_df = df.filter(F.size(F.col("_reasons")) > 0)
valid_df   = df.filter(F.size(F.col("_reasons")) == 0)
# warning_df is a subset of valid_df — these rows go to curated, not quarantine
warning_df = valid_df.filter(F.size(F.col("_warn_reasons")) > 0)

invalid_count = invalid_df.count()
valid_count   = valid_df.count()
warning_count = warning_df.count()

print(f"Valid rows   : {valid_count:,}  (includes {warning_count:,} with warnings)")
print(f"Invalid rows : {invalid_count:,}  (will be quarantined)")

# ── Duplicate detection ───────────────────────────────────────────────────────
# Counts total rows involved in duplicates, not the excess count
duplicate_transactionid_rows_total = int(
    valid_df
    .groupBy("transactionid")
    .count()
    .filter(F.col("count") > 1)
    .agg(F.sum("count").alias("total"))
    .collect()[0]["total"] or 0
)
print(f"Rows involved in duplicate transactionids: {duplicate_transactionid_rows_total:,}")

# ── Anomaly metrics ───────────────────────────────────────────────────────────
# These rows are NOT rejected — just counted for Lambda/agent to interpret
anomaly_high_amount_count   = valid_df.filter(
    F.col("amount_parsed") > AMOUNT_ANOMALY_THRESHOLD).count()
anomaly_high_quantity_count = valid_df.filter(
    F.col("quantity_parsed") > QUANTITY_ANOMALY_THRESHOLD).count()

# ── Amount and quantity stats ─────────────────────────────────────────────────
stats = valid_df.agg(
    F.min("amount_parsed").alias("amount_min"),
    F.max("amount_parsed").alias("amount_max"),
    F.round(F.avg("amount_parsed"), 2).alias("amount_avg"),
    F.min("quantity_parsed").alias("quantity_min"),
    F.max("quantity_parsed").alias("quantity_max"),
    F.round(F.avg("quantity_parsed"), 2).alias("quantity_avg"),
).collect()[0]

# ── Reason code breakdown ─────────────────────────────────────────────────────
reason_counts = (
    invalid_df
    .select(F.explode("_reasons").alias("reason"))
    .groupBy("reason").count()
    .collect()
)
reason_breakdown = {row["reason"]: row["count"] for row in reason_counts}

warn_reason_counts = (
    warning_df
    .select(F.explode("_warn_reasons").alias("reason"))
    .groupBy("reason").count()
    .collect()
)
warn_reason_breakdown = {row["reason"]: row["count"] for row in warn_reason_counts}

# ── Write invalid rows to quarantine ─────────────────────────────────────────
# Keep diagnostic columns so engineers can investigate failures
if invalid_count > 0:
    print(f"Writing {invalid_count:,} invalid rows to quarantine...")
    invalid_df \
        .withColumn("_invalid_reasons", F.array_join("_reasons", ",")) \
        .drop("_reasons", "_warn_reasons") \
        .write.mode("overwrite").parquet(QUARANTINE_PATH)

# ── Prepare curated DataFrame ─────────────────────────────────────────────────
# Valid rows (including warning rows) go to curated
curated_df = valid_df \
    .withColumn("transactionts", F.col("transactionts_parsed")) \
    .withColumn("amount",        F.col("amount_parsed")) \
    .withColumn("quantity",      F.col("quantity_parsed")) \
    .drop("transactionts_parsed", "amount_parsed", "quantity_parsed",
          "_reasons", "_warn_reasons")

# ── Write curated Parquet ─────────────────────────────────────────────────────
print(f"Writing curated Parquet to {CURATED_PATH}...")
curated_df \
    .repartition("dt") \
    .write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(CURATED_PATH)

# ── Build quality report ──────────────────────────────────────────────────────
invalid_rate = invalid_count / raw_count if raw_count > 0 else 0
warning_rate = warning_count / raw_count if raw_count > 0 else 0

# Glue does NOT make the batch decision here.
# Lambda reads this report and returns ALLOW, ALLOW_WITH_WARNING, ESCALATE, or QUARANTINE.

# Baseline from previous run — used by Lambda/agent to detect drift.
# In production this would be fetched from the previous report in S3.
# Mocked here as None until historical reports exist.
previous_invalid_rate = None
previous_warning_rate = None

dominant_invalid_reason = (
    max(reason_breakdown, key=reason_breakdown.get)
    if reason_breakdown else None
)

report = {
    "source_date":    SOURCE_DATE,
    "s3_key":         S3_KEY,
    "processed_at":   datetime.now(timezone.utc).isoformat(),
    "batch_status":   "PENDING_EVALUATION",
    "raw_count":      raw_count,
    "valid_count":    valid_count,
    "invalid_count":  invalid_count,
    "warning_count":  warning_count,
    "invalid_rate":   round(invalid_rate, 6),
    "warning_rate":   round(warning_rate, 6),
    "previous_invalid_rate": previous_invalid_rate,
    "previous_warning_rate": previous_warning_rate,
    "dominant_invalid_reason": dominant_invalid_reason,
    "duplicate_transactionid_rows_total": duplicate_transactionid_rows_total,
    "anomaly_high_amount_count":          anomaly_high_amount_count,
    "anomaly_high_amount_rate":           round(anomaly_high_amount_count / valid_count, 6) if valid_count > 0 else 0,
    "anomaly_high_quantity_count":        anomaly_high_quantity_count,
    "anomaly_high_quantity_rate":         round(anomaly_high_quantity_count / valid_count, 6) if valid_count > 0 else 0,
    "amount_min":     stats["amount_min"],
    "amount_max":     stats["amount_max"],
    "amount_avg":     stats["amount_avg"],
    "quantity_min":   stats["quantity_min"],
    "quantity_max":   stats["quantity_max"],
    "quantity_avg":   stats["quantity_avg"],
    "invalid_reason_breakdown": reason_breakdown,
    "warning_reason_breakdown": warn_reason_breakdown,
    "thresholds": {
        "max_invalid_rate":            MAX_INVALID_RATE,
        "max_warning_rate":            MAX_WARNING_RATE,
        "amount_anomaly_threshold":    AMOUNT_ANOMALY_THRESHOLD,
        "quantity_anomaly_threshold":  QUANTITY_ANOMALY_THRESHOLD,
    }
}

# ── Write quality report to S3 ────────────────────────────────────────────────
print(f"Writing quality report to s3://{BUCKET}/{REPORT_KEY}")
s3 = boto3.client("s3")
s3.put_object(
    Bucket=BUCKET,
    Key=REPORT_KEY,
    Body=json.dumps(report, indent=2).encode("utf-8"),
    ContentType="application/json"
)

print("\n── Quality Report ──────────────────────────────────────")
print(json.dumps(report, indent=2))
print("────────────────────────────────────────────────────────")
print("Done. Lambda evaluator will read the report and decide batch status.")

job.commit()
