"""
P005 - Event Driven Batch ETL Pipeline
Generate one day of synthetic transaction data and upload to S3 landing zone.

Simulates a daily CSV file drop from an upstream system.

Usage:
    python generate_daily_csv.py                  # defaults to today
    python generate_daily_csv.py 2026-03-18       # specific date

Requirements: pip install boto3 pandas numpy
"""

import boto3
import pandas as pd
import numpy as np
import io
import random
import string
import sys
from datetime import date

# ── Config ──────────────────────────────────────────────────────────────────
BUCKET       = "nadav-p005-pipeline"
LANDING_PREFIX = "landing/transactions"
ROWS_PER_DAY = 100_000

STORE_IDS   = [f"STORE_{i:03d}" for i in range(1, 51)]
PRODUCT_IDS = [f"PROD_{i:04d}" for i in range(1, 201)]
# ────────────────────────────────────────────────────────────────────────────

def make_transaction_id():
    return "TXN" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))

def generate_day(date_str):
    """Generate one day of transaction records."""
    n = ROWS_PER_DAY
    return pd.DataFrame({
        "transactionid": [make_transaction_id() for _ in range(n)],
        "transactionts": [
            f"{date_str}T{h:02d}:{m:02d}:{s:02d}"
            for h, m, s in zip(
                np.random.randint(0, 24, n),
                np.random.randint(0, 60, n),
                np.random.randint(0, 60, n),
            )
        ],
        "storeid":   np.random.choice(STORE_IDS,   n),
        "productid": np.random.choice(PRODUCT_IDS, n),
        "amount":    np.round(np.random.uniform(1.0, 500.0, n), 2),
        "quantity":  np.random.randint(1, 10, n),
    })

def upload(df, date_str):
    """Upload DataFrame as CSV to S3 landing zone."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")

    key = f"{LANDING_PREFIX}/dt={date_str}/transactions.csv"
    size_mb = len(csv_bytes) / (1024 ** 2)

    s3 = boto3.client("s3")
    print(f"Uploading {size_mb:.1f} MB to s3://{BUCKET}/{key} ...")
    s3.put_object(Bucket=BUCKET, Key=key, Body=csv_bytes)
    print(f"Done. {len(df):,} rows uploaded.")
    return key

if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else str(date.today())
    print(f"Generating data for {date_str}...")
    df = generate_day(date_str)
    upload(df, date_str)
