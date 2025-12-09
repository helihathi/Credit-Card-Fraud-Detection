#!/usr/bin/env python3

import argparse
import os
import sqlite3
import pandas as pd
import sys
from datetime import datetime, timedelta

DEFAULT_DB = os.path.join(os.path.dirname(__file__), "data", "fraud_detection.db")
DEFAULT_INPUT = "data/creditcard.csv"


def load_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} rows from {path}")
    return df


def normalize_dataframe(df):
    # Standardize column names (strip, remove BOM, etc.)
    df.columns = [c.strip() for c in df.columns]

    # If the file has "Class" use it as is_fraud
    if "Class" in df.columns and "is_fraud" not in df.columns:
        df = df.rename(columns={"Class": "is_fraud"})

    # If the file has lowercase 'amount' or 'Amount', standardize to 'Amount'
    if "amount" in df.columns and "Amount" not in df.columns:
        df = df.rename(columns={"amount": "Amount"})

    # If file does not have Time, optionally create one (sequential)
    if "Time" not in df.columns:
        # create a synthetic Time if necessary (seconds from 0)
        df["Time"] = range(len(df))

    # Ensure V1..V28 exist (if not, raise)
    missing_vs = [f"V{i}" for i in range(1, 29) if f"V{i}" not in df.columns]
    if missing_vs:
        raise ValueError(f"Missing required columns: {missing_vs}. Please provide a dataset with V1..V28.")

    # Ensure Amount exists
    if "Amount" not in df.columns:
        raise ValueError("Missing 'Amount' column in dataset.")

    # Ensure is_fraud exists; if not, fill with 0
    if "is_fraud" not in df.columns:
        print("Warning: 'is_fraud' not found in input — filling with 0 (non-fraud).")
        df["is_fraud"] = 0

    # Reorder to canonical order: Time, V1..V28, Amount, is_fraud, (others)
    canonical = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount", "is_fraud"]
    # keep any extra columns that exist (like Amount_log, Hour, metadata) after the canonical ones
    extras = [c for c in df.columns if c not in canonical]
    df = df[canonical + extras]

    return df


def prepare_rows_for_db(df, db_columns):
    """
    Map dataframe columns to DB table columns.
    db_columns: list of column names in the transactions table (ordered)
    Returns list of tuples for executemany insert.
    """
    rows = []
    n = len(df)

    # Generate transaction_id if not present
    if "transaction_id" not in df.columns:
        df.insert(0, "transaction_id", [f"TX{i:06d}" for i in range(1, n + 1)])
    else:
        # ensure unique-ish transaction ids
        df["transaction_id"] = df["transaction_id"].astype(str)

    # Ensure timestamp column exists in df if DB wants it
    if "timestamp" in db_columns and "timestamp" not in df.columns:
        # try to create a timestamp from Time (assuming Time in seconds)
        try:
            base = datetime(2025, 1, 1)
            df["timestamp"] = df["Time"].apply(lambda s: (base + timedelta(seconds=float(s))).isoformat())
        except Exception:
            df["timestamp"] = None

    # Now for each row, produce values ordered as db_columns
    for _, r in df.iterrows():
        vals = []
        for col in db_columns:
            if col in r.index:
                val = r[col]
                # Force Python None for NaN
                if pd.isna(val):
                    val = None
                vals.append(val)
            else:
                vals.append(None)
        rows.append(tuple(vals))
    return rows


def run_etl(input_csv, db_path=DEFAULT_DB, clear_existing=True):
    print(f"ETL starting. input={input_csv}, db={db_path}")
    df = load_csv(input_csv)
    df = normalize_dataframe(df)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Get transactions table columns
    cur.execute("PRAGMA table_info(transactions);")
    cols_info = cur.fetchall()
    if not cols_info:
        conn.close()
        raise RuntimeError("The 'transactions' table does not exist. Run create_db.py first.")

    db_columns = [col[1] for col in cols_info]  # second field is name
    print("Transactions table columns:", db_columns)

    # Optionally clear existing rows
    if clear_existing:
        print("Clearing existing rows in transactions table...")
        cur.execute("DELETE FROM transactions;")
        conn.commit()

    # Prepare rows mapped to DB columns
    rows = prepare_rows_for_db(df, db_columns)
    placeholders = ",".join(["?"] * len(db_columns))
    insert_sql = f"INSERT INTO transactions ({','.join(db_columns)}) VALUES ({placeholders});"

    print(f"Inserting {len(rows)} rows into transactions...")
    # Insert in chunked batches for memory safety
    batch_size = 1000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cur.executemany(insert_sql, batch)
        conn.commit()
        print(f"  inserted rows {i + 1} .. {i + len(batch)}")

    conn.close()
    print("ETL completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL: populate transactions table from CSV")
    parser.add_argument("--input", type=str, default=DEFAULT_INPUT,
                        help="Path to input CSV file (default: /mnt/data/creditcard.csv)")
    parser.add_argument("--db", type=str, default=DEFAULT_DB, help="Path to SQLite DB (default fraud_detection.db)")
    parser.add_argument("--no-clear", action="store_true", help="Do not delete existing transactions rows (append instead)")
    args = parser.parse_args()

    run_etl(args.input, db_path=args.db, clear_existing=(not args.no_clear))