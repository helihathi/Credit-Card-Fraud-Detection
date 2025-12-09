# ============================================================
# create_db.py – Initialize Database Schema
# ============================================================
import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "fraud_detection.db")

def create_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # -----------------------------------------------
    # 1️⃣ transactions – Clean dataset from ETL (Producer input)
    # -----------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id TEXT PRIMARY KEY,
        Time REAL,
        Amount REAL,
        V1 REAL, V2 REAL, V3 REAL, V4 REAL, V5 REAL, 
        V6 REAL, V7 REAL, V8 REAL, V9 REAL, V10 REAL,
        V11 REAL, V12 REAL, V13 REAL, V14 REAL, V15 REAL,
        V16 REAL, V17 REAL, V18 REAL, V19 REAL, V20 REAL,
        V21 REAL, V22 REAL, V23 REAL, V24 REAL, V25 REAL,
        V26 REAL, V27 REAL, V28 REAL,
        is_fraud INTEGER,
        customer_id TEXT,
        merchant_id TEXT,
        location TEXT,
        device TEXT,
        channel TEXT,
        timestamp TEXT
    );
    """)

    # -----------------------------------------------
    # 2️⃣ transactions_log – Real-time scoring output
    # -----------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id TEXT,
        fraud_probability REAL,
        is_fraud INTEGER,
        amount REAL,
        processed_at TEXT,
        model_version TEXT
    );
    """)

    # Indexes for faster dashboard queries
    cur.execute("CREATE INDEX IF NOT EXISTS idx_log_txid ON transactions_log(transaction_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_log_processed ON transactions_log(processed_at);")

    # -----------------------------------------------
    # 3️⃣ alerts – Registered alert recipients
    # -----------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE,
        active INTEGER DEFAULT 1,
        notify_threshold REAL DEFAULT 0.8
    );
    """)

    # -----------------------------------------------
    # 4️⃣ alerts_history – Logged alert events
    # -----------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS alerts_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id TEXT,
        fraud_probability REAL,
        sent_to TEXT,
        sent_at TEXT,
        status TEXT,
        error_message TEXT
    );
    """)

    # -----------------------------------------------
    # 5️⃣ model_info – Track model version deployed
    # -----------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS model_info (
        version TEXT PRIMARY KEY,
        path TEXT,
        created_at TEXT,
        notes TEXT
    );
    """)

    # -----------------------------------------------
    # 6️⃣ stream_health – Monitor system health
    # -----------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stream_health (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_time TEXT,
        producer_status TEXT,
        consumer_status TEXT,
        latency_ms REAL
    );
    """)

    conn.commit()
    conn.close()
    print("Database initialized")


if __name__ == "__main__":
    create_tables()
