#!/usr/bin/env python3
import sqlite3
import argparse
import logging
import shutil
import os
import sys
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("migrate_db")

DEFAULT_DB = os.path.join(os.path.dirname(__file__), "data", "fraud_detection.db")

def backup_db(path: str) -> str:
    if not os.path.exists(path):
        logger.warning("DB file %s does not exist — nothing to backup", path)
        return ""
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dest = f"{path}.backup.{ts}"
    shutil.copy2(path, dest)
    logger.info("Backup created: %s", dest)
    return dest

def table_has_column(conn, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(%s);" % table)
    cols = [r[1] for r in cur.fetchall()]
    return column in cols

def add_column_if_missing(conn, table: str, column: str, col_def: str):
    if table_has_column(conn, table, column):
        logger.info("Table %s already has column %s — skipping", table, column)
        return
    sql = f"ALTER TABLE {table} ADD COLUMN {column} {col_def};"
    logger.info("Adding column %s to table %s", column, table)
    conn.execute(sql)
    logger.info("Added column %s", column)

def create_alerts_history(conn):
    logger.info("Ensuring alerts_history table exists")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT,
            fraud_score REAL,
            sent_to TEXT,
            sent_at TEXT DEFAULT (datetime('now')),
            status TEXT
        )
    """)
    # index for faster lookups
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_history_transaction_id ON alerts_history(transaction_id);")
    except Exception as e:
        logger.warning("Failed to create index on alerts_history.transaction_id: %s", e)

def ensure_transactions_log(conn):
    logger.info("Ensuring transactions_log table exists (minimal structure if missing)")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS transactions_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        transaction_id TEXT,
        fraud_probability REAL,
        is_fraud INTEGER,
        amount REAL
    );
    """)
    # optional index
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_log_transaction_id ON transactions_log(transaction_id);")
    except Exception as e:
        logger.warning("Failed to create index on transactions_log.transaction_id: %s", e)

def ensure_migrations_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            applied_at TEXT
        );
    """)

def migration_already_applied(conn, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM migrations WHERE name = ? LIMIT 1;", (name,))
    return cur.fetchone() is not None

def record_migration(conn, name: str):
    conn.execute("INSERT OR IGNORE INTO migrations(name, applied_at) VALUES (?, datetime('now'));", (name,))

def main(db_path: str, do_backup: bool):
    if do_backup:
        backup_db(db_path)

    if not os.path.exists(db_path):
        logger.info("Database %s not found; it will be created.", db_path)

    # connect with foreign keys enabled
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        ensure_migrations_table(conn)

        migration_name = "add_processed_at_and_model_version_and_alerts_history_v1"
        if migration_already_applied(conn, migration_name):
            logger.info("Migration '%s' already applied — exiting.", migration_name)
            return

        # Use a transaction so changes are atomic
        try:
            logger.info("Beginning migration transaction")
            conn.execute("BEGIN")

            ensure_transactions_log(conn)

            # Add processed_at with default current timestamp for future rows.
            # Keep it nullable so existing rows are not forced to have a value.
            add_column_if_missing(conn, "transactions_log", "processed_at", "TEXT DEFAULT (datetime('now'))")
            add_column_if_missing(conn, "transactions_log", "model_version", "TEXT")

            create_alerts_history(conn)

            # Optionally, create a foreign key if you have consistent transaction_id on both sides.
            # Note: creating a foreign key on an existing table in SQLite requires table rewrite (complex).
            # So we only create indexes here for performance.

            # mark migration as applied
            record_migration(conn, migration_name)

            conn.commit()
            logger.info("Migration committed successfully.")
        except Exception as e:
            logger.exception("Migration failed, rolling back: %s", e)
            conn.rollback()
            raise
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default=DEFAULT_DB, help="Path to SQLite DB")
    parser.add_argument("--no-backup", action="store_true", help="Skip creating a backup copy")
    args = parser.parse_args()
    try:
        main(args.db, do_backup=not args.no_backup)
        logger.info("Done.")
    except Exception as e:
        logger.error("Migration terminated with error: %s", e)
        sys.exit(2)