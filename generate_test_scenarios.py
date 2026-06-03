"""
P005 - Generate test scenario CSVs to verify all 4 Lambda decision paths.

Scenarios:
  clean      → ALLOW            (good data, all rates under threshold)
  warning    → ALLOW_WITH_WARNING (>10% missing storeid/productid)
  duplicates → ESCALATE          (>1% duplicate transactionids)
  corrupt    → QUARANTINE        (>5% null/invalid critical fields)

Usage:
    python generate_test_scenarios.py clean      2026-03-20
    python generate_test_scenarios.py warning    2026-03-21
    python generate_test_scenarios.py duplicates 2026-03-22
    python generate_test_scenarios.py corrupt    2026-03-23
    python generate_test_scenarios.py all
"""

import boto3
import pandas as pd
import numpy as np
import io
import random
import string
import sys
from datetime import date

BUCKET         = "nadav-p005-pipeline"
LANDING_PREFIX = "landing/transactions"
ROWS           = 100_000

STORE_IDS   = [f"STORE_{i:03d}" for i in range(1, 51)]
PRODUCT_IDS = [f"PROD_{i:04d}" for i in range(1, 201)]


def make_txn_id():
    return "TXN" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))


def base_df(date_str, n=ROWS):
    """Generate a clean base DataFrame."""
    return pd.DataFrame({
        "transactionid": [make_txn_id() for _ in range(n)],
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


def scenario_clean(date_str):
    """All rows valid — expect ALLOW."""
    return base_df(date_str)


def scenario_warning(date_str):
    """15% of rows have null storeid or productid — expect ALLOW_WITH_WARNING."""
    df = base_df(date_str)
    warn_idx = df.sample(frac=0.15).index
    df.loc[warn_idx[:len(warn_idx)//2], "storeid"]   = None
    df.loc[warn_idx[len(warn_idx)//2:], "productid"] = None
    return df


def scenario_duplicates(date_str):
    """3% of rows are duplicate transactionids — expect ESCALATE."""
    df = base_df(date_str)
    dup_count = int(ROWS * 0.03)
    dup_rows  = df.sample(dup_count).copy()
    df.iloc[-dup_count:] = dup_rows.values
    return df


def scenario_corrupt(date_str):
    """8% of rows have null transactionid or unparseable amount — expect QUARANTINE."""
    df = base_df(date_str)
    corrupt_idx = df.sample(frac=0.08).index
    df.loc[corrupt_idx[:len(corrupt_idx)//2], "transactionid"] = None
    df.loc[corrupt_idx[len(corrupt_idx)//2:], "amount"]        = "NOT_A_NUMBER"
    return df


SCENARIOS = {
    "clean":      ("2026-03-20", scenario_clean),
    "warning":    ("2026-03-21", scenario_warning),
    "duplicates": ("2026-03-22", scenario_duplicates),
    "corrupt":    ("2026-03-23", scenario_corrupt),
}


def upload(df, date_str):
    buf  = io.StringIO()
    df.to_csv(buf, index=False)
    body = buf.getvalue().encode("utf-8")
    key  = f"{LANDING_PREFIX}/dt={date_str}/transactions.csv"
    s3   = boto3.client("s3")
    print(f"  Uploading {len(body)/1024/1024:.1f} MB → s3://{BUCKET}/{key}")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    print(f"  Done. {len(df):,} rows.")


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"

    if arg == "all":
        targets = list(SCENARIOS.keys())
    elif arg in SCENARIOS:
        targets = [arg]
    else:
        print(f"Unknown scenario '{arg}'. Choose from: {list(SCENARIOS.keys())} or 'all'")
        sys.exit(1)

    for name in targets:
        default_date, fn = SCENARIOS[name]
        date_str = sys.argv[2] if (len(sys.argv) > 2 and arg != "all") else default_date
        print(f"\n[{name.upper()}] date={date_str}")
        df = fn(date_str)
        upload(df, date_str)

    print("\nAll uploads complete. Watch Step Functions for executions.")
