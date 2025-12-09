import os
import json
import time
import joblib
import sqlite3
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
import pandas as pd
import numpy as np
from typing import Optional

# Import send_alert from your alert_email.py (should exist)
from alert_email import send_alert

# -------------------
# Logging
# -------------------
logger = logging.getLogger("consumer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# -------------------
# Env / config
# -------------------
# Use container-mounted path by default (adjust via DB_PATH env if needed)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "fraud-consumer-group")
MODEL_PATH = os.getenv("MODEL_PATH", "model/xgboost_caliberated.joblib")
SCALER_PATH = os.getenv("SCALER_PATH", "model/standard_scaler.joblib")
DB_PATH = os.getenv("DB_PATH", "/app/data/fraud_detection.db")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.001"))
MODEL_VERSION = os.getenv("MODEL_VERSION", os.path.basename(MODEL_PATH))
POLL_TIMEOUT = float(os.getenv("POLL_TIMEOUT", "1.0"))  # seconds

# -------------------
# Helper: feature engineering
# -------------------
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Standardize Amount column
    if "Amount" not in df.columns and "amount" in df.columns:
        df["Amount"] = df["amount"]
    if "Amount" in df.columns and "Amount_log" not in df.columns:
        # safe log transform
        df["Amount_log"] = np.log1p(pd.to_numeric(df["Amount"], errors="coerce").fillna(0.0))
    # Hour from Time if present
    if "Time" in df.columns:
        try:
            df["Hour"] = (pd.to_numeric(df["Time"], errors="coerce").fillna(0).astype(int) // 3600) % 24
        except Exception:
            df["Hour"] = 0
    else:
        df["Hour"] = 0
    return df

# -------------------
# Prepare expected feature order used in training
# -------------------
EXPECTED_FEATURES = [
    "V1","V2","V3","V4","V5","V6","V7","V8","V9","V10",
    "V11","V12","V13","V14","V15","V16","V17","V18","V19","V20",
    "V21","V22","V23","V24","V25","V26","V27","V28",
    "Amount","Amount_log","Hour"
]

# -------------------
# Load model & scaler
# -------------------
model = None
scaler = None

def load_model_and_scaler():
    global model, scaler
    # Load model
    try:
        logger.info("Loading model from %s", MODEL_PATH)
        model = joblib.load(MODEL_PATH)
        logger.info("Model loaded successfully (type=%s)", type(model))
    except Exception as e:
        logger.exception("Failed to load model from %s: %s", MODEL_PATH, e)
        raise

    # Load scaler if available
    if os.path.exists(SCALER_PATH):
        try:
            logger.info("Loading scaler from %s", SCALER_PATH)
            scaler = joblib.load(SCALER_PATH)
            logger.info("Scaler loaded successfully (type=%s)", type(scaler))
        except Exception as e:
            logger.exception("Failed to load scaler from %s: %s", SCALER_PATH, e)
            scaler = None
    else:
        logger.info("No scaler found at %s, proceeding without scaler", SCALER_PATH)
        scaler = None

# -------------------
# Prediction helper (support pipelines, sklearn models, xgboost)
# -------------------
def predict_probability(X_df: pd.DataFrame) -> float:
    """
    Return fraud probability for the positive class (index 1).
    Attempts multiple ways to predict in order:
      - model.predict_proba(X)
      - if model has named_steps['model'] etc
      - if model is xgboost.Booster: use DMatrix + predict
    """
    if model is None:
        raise RuntimeError("Model is not loaded")

    # If scaler exists, transform numeric features (we only transform the expected features)
    X = X_df.copy()

    # Reindex to expected_features, fill missing with 0
    X = X.reindex(columns=EXPECTED_FEATURES).fillna(0)

    # Optionally use scaler (scaler expects 2D array)
    if scaler is not None:
        try:
            arr = scaler.transform(X)
            # model may expect dataframe or array; convert to DataFrame with same columns
            X = pd.DataFrame(arr, columns=EXPECTED_FEATURES)
        except Exception as e:
            logger.warning("Scaler transform failed: %s — continuing with unscaled features", e)

    # Try predict_proba
    try:
        # If model is a Pipeline with named_steps['model'] (sklearn-like)
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba(X)
            # ensure shape
            if proba.ndim == 2 and proba.shape[1] >= 2:
                return float(proba[0, 1])
            # fallback to first column
            return float(proba[0])
    except Exception as e:
        logger.debug("model.predict_proba failed: %s", e)

    # Try pipeline access (if pipeline has named_steps)
    try:
        if hasattr(model, "named_steps") and "model" in model.named_steps:
            m = model.named_steps["model"]
            if hasattr(m, "predict_proba"):
                proba = m.predict_proba(X)
                if proba.ndim == 2 and proba.shape[1] >= 2:
                    return float(proba[0, 1])
    except Exception as e:
        logger.debug("pipeline inner model predict_proba failed: %s", e)

    # Try xgboost Booster fallback
    try:
        # xgboost Booster expects DMatrix
        import xgboost as xgb
        if isinstance(model, xgb.Booster):
            dmat = xgb.DMatrix(X.values, feature_names=EXPECTED_FEATURES)
            preds = model.predict(dmat)
            # booster returns probability array; for single row return preds[0]
            return float(preds[0])
    except Exception as e:
        logger.debug("xgboost.Booster fallback failed: %s", e)

    raise RuntimeError("Could not obtain probability from loaded model.")

# -------------------
# DB helpers
# -------------------
def ensure_db_schema(conn: sqlite3.Connection):
    # Kept for reference but NOT called by consumer; create_db.py must create schema
    cur = conn.cursor()
    # transactions_log with processed_at & model_version
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id TEXT,
        fraud_probability REAL,
        is_fraud INTEGER,
        amount REAL,
        processed_at TEXT,
        model_version TEXT
    )
    """)
    # alerts_history table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS alerts_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id TEXT,
        fraud_score REAL,
        sent_to TEXT,
        sent_at TEXT,
        status TEXT
    )
    """)
    conn.commit()

def log_transaction(conn: sqlite3.Connection, txid: str, prob: float, flagged: bool, amount: Optional[float]):
    """
    Insert transaction row with retry on sqlite 'database is locked' errors.
    """
    cur = conn.cursor()
    processed_at = datetime.utcnow().isoformat()

    sql = """
        INSERT INTO transactions_log (transaction_id, fraud_probability, is_fraud, amount, processed_at, model_version)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    params = (txid, float(prob), int(flagged), float(amount) if amount is not None else None, processed_at, MODEL_VERSION)

    max_retries = 5
    backoff = 0.1
    for attempt in range(max_retries):
        try:
            cur.execute(sql, params)
            conn.commit()
            logger.debug("Inserted tx=%s prob=%.6f flagged=%s processed_at=%s", txid, prob, flagged, processed_at)
            return
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "database is locked" in msg or "database table is locked" in msg:
                wait = backoff * (2 ** attempt)
                logger.warning("SQLite locked on insert (attempt %d/%d). Retrying in %.3fs", attempt + 1, max_retries, wait)
                time.sleep(wait)
                continue
            else:
                logger.exception("OperationalError writing to DB (non-lock): %s", e)
                raise
        except Exception as e:
            logger.exception("Unexpected error writing to DB: %s", e)
            raise

    logger.error("Failed to insert tx=%s after %d attempts due to persistent SQLite lock.", txid, max_retries)
    raise sqlite3.OperationalError("Failed to write to DB after retries (database locked)")

def log_alert_history(conn: sqlite3.Connection, txid: str, prob: float, sent_to: str, status: str):
    cur = conn.cursor()
    sent_at = datetime.utcnow().isoformat()
    sql = """
        INSERT INTO alerts_history (transaction_id, fraud_score, sent_to, sent_at, status)
        VALUES (?, ?, ?, ?, ?)
    """
    params = (txid, float(prob), sent_to, sent_at, status)

    max_retries = 5
    backoff = 0.1
    for attempt in range(max_retries):
        try:
            cur.execute(sql, params)
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                wait = backoff * (2 ** attempt)
                logger.warning("SQLite locked (alerts_history) retry %d/%d — waiting %.3fs", attempt+1, max_retries, wait)
                time.sleep(wait)
                continue
            else:
                logger.exception("OperationalError writing alert history: %s", e)
                raise
        except Exception as e:
            logger.exception("Unexpected error writing alert history: %s", e)
            raise
    logger.error("Failed to insert alert history for %s after %d attempts", txid, max_retries)

# -------------------
# Kafka consumer loop
# -------------------
def run_consumer():
    # Setup DB with safer connection settings and PRAGMAs
    # (timeout and check_same_thread reduce lock errors; PRAGMA WAL reduces contention)
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    try:
        cur = conn.cursor()
        # enable WAL + reasonable sync + set busy timeout (milliseconds)
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA busy_timeout=30000;")
        conn.commit()
    except Exception:
        logger.exception("Failed to set sqlite PRAGMAs (continuing)")

    # DO NOT create schema here — create_db.py should do that
    # ensure_db_schema(conn)  # intentionally NOT called

    # Setup Kafka consumer
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        # increase session timeout if needed
        "session.timeout.ms": 60000,
    }
    consumer = None
    try:
        logger.info("Connecting to Kafka %s topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC])
    except Exception as e:
        logger.exception("Failed to create Kafka consumer: %s", e)
        raise

    try:
        while True:
            try:
                msg = consumer.poll(POLL_TIMEOUT)
            except KafkaException as ke:
                logger.warning("Kafka exception while polling: %s", ke)
                time.sleep(2)
                continue
            except Exception as e:
                logger.exception("Unexpected error during consumer.poll: %s", e)
                time.sleep(2)
                continue

            if msg is None:
                # no message
                continue
            if msg.error():
                logger.warning("Kafka message error: %s", msg.error())
                continue

            try:
                raw = msg.value()
                if raw is None:
                    continue
                record = json.loads(raw.decode("utf-8")) if isinstance(raw, (bytes, bytearray)) else json.loads(raw)
            except Exception as e:
                logger.exception("Failed to decode message: %s", e)
                continue

            # get fields
            txid = record.get("transaction_id", f"tx_{int(time.time() * 1000)}")
            amount = record.get("Amount", record.get("amount", None))
            try:
                # Make DataFrame for prediction
                df = pd.DataFrame([record])
                df = add_features(df)
                # Keep only expected features (reindex will add missing columns)
                df = df.reindex(columns=EXPECTED_FEATURES).fillna(0)

                prob = predict_probability(df)
                flagged = prob > FRAUD_THRESHOLD

                # Log (with retry on locked implemented above)
                try:
                    log_transaction(conn, txid, prob, flagged, amount)
                except Exception as e:
                    logger.exception("Failed to log transaction to DB: %s", e)

                # Print
                status = "FRAUD" if flagged else "SAFE"
                logger.info("TX %s | amount=%s | prob=%.6f | %s", txid, amount, prob, status)

                # Send email alert if flagged
                if flagged:
                    try:
                        success = send_alert(txid, prob, amount)
                        log_alert_history(conn, txid, prob, os.getenv("ALERT_RECEIVER", "unknown"), "sent" if success else "failed")
                    except Exception as e:
                        logger.exception("Failed to send alert for %s: %s", txid, e)
                        try:
                            log_alert_history(conn, txid, prob, os.getenv("ALERT_RECEIVER", "unknown"), "exception")
                        except Exception:
                            pass

            except Exception as e:
                logger.exception("Error processing message: %s", e)
                continue

    except KeyboardInterrupt:
        logger.info("Interrupted by user — closing consumer.")
    finally:
        try:
            if consumer:
                consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

# -------------------
# Main entry
# -------------------
if __name__ == "__main__":
    try:
        load_model_and_scaler()
    except Exception as e:
        logger.error("Exiting due to model/scaler load failure: %s", e)
        raise SystemExit(1)

    run_consumer()
