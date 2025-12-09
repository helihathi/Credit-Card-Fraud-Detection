import argparse
import json
import time
import random
import string
import sys
import signal
import sqlite3
import os   # <— added for env fallback
from typing import Dict, Any, Iterator, List
import pandas as pd
import numpy as np
from confluent_kafka import Producer

# ---------------------------
# Helpers & configuration
# ---------------------------

# Now uses env variables inside Docker (minimal change)
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = "transactions"
DEFAULT_DB = os.getenv("DB_PATH", "fraud_detection.db")

stop_requested = False


def handle_sigint(signum, frame):
    global stop_requested
    stop_requested = True


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


def json_serializer(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, default=lambda x: None).encode("utf-8")


# ---------------------------
# Random transaction generator
# ---------------------------
def generate_random_transaction() -> Dict[str, Any]:
    transaction_id = "TX" + "".join(random.choices(string.digits, k=6))
    amount = round(random.uniform(1.0, 5000.0), 2)
    time_val = random.randint(0, 172800)

    features = {f"V{i}": float(round(np.random.randn(), 4)) for i in range(1, 29)}

    fraud_flag = 1 if (amount > 3000 and features["V17"] < -1) else 0

    transaction = {
        "transaction_id": transaction_id,
        "Time": time_val,
        "Amount": amount,
        "is_fraud": int(fraud_flag),
        **features
    }
    return transaction


# ---------------------------
# Dataset iterator (from SQLite)
# ---------------------------
def iter_transactions_from_db(db_path: str, table: str = "transactions", shuffle: bool = False) -> Iterator[Dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT * FROM {table};", conn)
    conn.close()

    if df.empty:
        raise RuntimeError(f"No rows found in {table} (db={db_path}). Run ETL first.")

    expected = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount", "is_fraud"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise RuntimeError(f"Dataset is missing required columns: {missing}")

    if shuffle:
        df = df.sample(frac=1).reset_index(drop=True)

    if "transaction_id" not in df.columns:
        df.insert(0, "transaction_id", [f"TX{i:06d}" for i in range(1, len(df) + 1)])
    else:
        df["transaction_id"] = df["transaction_id"].astype(str)

    for _, row in df.iterrows():
        rec = row.to_dict()
        for k, v in list(rec.items()):
            if pd.isna(v):
                rec[k] = None
            elif isinstance(v, (np.integer,)):
                rec[k] = int(v)
            elif isinstance(v, (np.floating,)):
                rec[k] = float(v)
        yield rec


# ---------------------------
# Kafka producer wrapper
# ---------------------------
class KafkaProducerWrapper:
    def __init__(self, bootstrap: str, linger_ms: int = 10):
        conf = {"bootstrap.servers": bootstrap, "linger.ms": linger_ms}
        self.producer = Producer(conf)

    def send(self, topic: str, key: str, value: Dict[str, Any], headers: List[tuple] = None):

        k = key.encode("utf-8") if key is not None else None
        v = json_serializer(value)

        # Minimal, necessary: add delivery callback
        def _delivery_report(err, msg):
            if err is not None:
                print(f"[producer] Delivery failed for {msg.key()}: {err}", file=sys.stderr)

        self.producer.produce(topic, key=k, value=v, headers=headers, callback=_delivery_report)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush(timeout=10)


# ---------------------------
# Main streaming loop
# ---------------------------
def stream_dataset_mode(args):
    print(f"Starting producer. db={args.db} topic={args.topic} rate={args.rate} msg/s loop={args.loop} shuffle={args.shuffle}")

    iterator = iter_transactions_from_db(args.db, shuffle=args.shuffle)

    producer = None
    if not args.dry_run:
        producer = KafkaProducerWrapper(args.bootstrap)

    sent = 0
    start_time = time.time()

    while True:
        for record in iterator:
            if stop_requested:
                print("Stop requested — exiting.")
                if producer:
                    producer.flush()
                return

            out = {
                "transaction_id": record.get("transaction_id"),
                "Time": record.get("Time"),
                "Amount": record.get("Amount"),
                "is_fraud": int(record.get("is_fraud")) if record.get("is_fraud") is not None else 0
            }
            for i in range(1, 29):
                out[f"V{i}"] = record.get(f"V{i}")

            if args.dry_run:
                print("DRY RUN:", out)
            else:
                producer.send(args.topic, key=out["transaction_id"], value=out)

            sent += 1
            if args.rate and args.rate > 0:
                time.sleep(1.0 / args.rate)

            if args.limit and sent >= args.limit:
                print(f"Reached send limit ({args.limit}). Flushing and exiting.")
                producer.flush()
                return

        if args.loop:
            iterator = iter_transactions_from_db(args.db, shuffle=args.shuffle)
            print("Looping dataset again...")
            continue
        else:
            if producer:
                producer.flush()
            print("Completed dataset streaming.")
            return


def stream_random_mode(args):
    print(f"Starting producer in RANDOM mode. topic={args.topic} rate={args.rate} msg/s")
    producer = None
    if not args.dry_run:
        producer = KafkaProducerWrapper(args.bootstrap)

    sent = 0
    while not stop_requested:
        rec = generate_random_transaction()
        if args.dry_run:
            print("DRY RUN:", rec)
        else:
            producer.send(args.topic, key=rec["transaction_id"], value=rec)

        sent += 1
        if args.rate and args.rate > 0:
            time.sleep(1.0 / args.rate)

        if args.limit and sent >= args.limit:
            print(f"Reached send limit ({args.limit}). Flushing and exiting.")
            producer.flush()
            return

    if producer:
        producer.flush()


# ---------------------------
# CLI and entrypoint
# ---------------------------
def build_arg_parser():
    p = argparse.ArgumentParser(description="Kafka Producer for Fraud Detection (dataset or random mode)")
    p.add_argument("--mode", choices=["dataset", "random"], default="dataset")
    p.add_argument("--db", type=str, default=DEFAULT_DB)
    p.add_argument("--topic", type=str, default=DEFAULT_TOPIC)
    p.add_argument("--bootstrap", type=str, default=DEFAULT_BOOTSTRAP)
    p.add_argument("--rate", type=float, default=1.0)
    p.add_argument("--loop", action="store_true")
    p.add_argument("--shuffle", action="store_true")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    return p


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        if args.mode == "dataset":
            stream_dataset_mode(args)
        else:
            stream_random_mode(args)
    except Exception as e:
        print("Producer encountered an error:", e, file=sys.stderr)
        raise
    finally:
        # global safe flush
        try:
            # This attempts to flush any producer created in the stream functions
            KafkaProducerWrapper(DEFAULT_BOOTSTRAP).flush()
        except Exception:
            pass


if __name__ == "__main__":
    main()