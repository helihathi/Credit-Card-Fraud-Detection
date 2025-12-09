# app.py — FastAPI backend (container-ready)
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import pandas as pd
import sqlite3
import uvicorn
import numpy as np
import os
from typing import Optional
from datetime import datetime

MODEL_PATH = os.getenv("MODEL_PATH", "/app/model/xgboost_caliberated.joblib")
SCALER_PATH = os.getenv("SCALER_PATH", "/app/model/standard_scaler.joblib")
DB_PATH = os.getenv("DB_PATH", "/app/data/fraud_detection.db")  # changed default to mounted data dir
MODEL_VERSION = os.getenv("MODEL_VERSION", "xgboost_caliberated")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.001"))

# Load model & scaler at startup
def robust_load_model(path):
    try:
        m = joblib.load(path)
        return m
    except Exception as e:
        raise RuntimeError(f"Failed to load model from {path}: {e}")

def robust_load_scaler(path):
    if os.path.exists(path):
        try:
            s = joblib.load(path)
            return s
        except Exception:
            return None
    return None

# attempt to load model/scaler; if these fail the app will raise at startup (intentional)
model = robust_load_model(MODEL_PATH)
scaler = robust_load_scaler(SCALER_PATH)

# Helper predict util (mirrors consumer logic — supports pipeline/xgb booster)
def predict_proba_array(X):
    X = np.asarray(X, dtype=float)
    # sklearn predict_proba
    try:
        if hasattr(model, "predict_proba"):
            return model.predict_proba(X)[:, 1]
    except Exception:
        pass
    try:
        if hasattr(model, "named_steps") and "model" in model.named_steps:
            inner = model.named_steps["model"]
            if hasattr(inner, "predict_proba"):
                return inner.predict_proba(X)[:, 1]
    except Exception:
        pass
    try:
        import xgboost as xgb
        if isinstance(model, xgb.Booster):
            d = xgb.DMatrix(X)
            p = model.predict(d)
            if getattr(p, "ndim", 1) == 2:
                return p[:, -1]
            return p
    except Exception:
        pass
    try:
        if hasattr(model, "predict"):
            p = np.asarray(model.predict(X), dtype=float)
            if set(np.unique(p)).issubset({0.0, 1.0}):
                return np.clip(p * 0.98 + 0.01, 0.0, 1.0)
            return np.clip(p, 0.0, 1.0)
    except Exception:
        pass
    return np.zeros(X.shape[0], dtype=float)

# FastAPI setup
app = FastAPI(title="Financial Fraud Detection API", version="1.0")

# Pydantic input model: allow optional Time and Hour
class Transaction(BaseModel):
    Time: Optional[float] = 0
    V1: float; V2: float; V3: float; V4: float; V5: float; V6: float; V7: float; V8: float; V9: float; V10: float
    V11: float; V12: float; V13: float; V14: float; V15: float; V16: float; V17: float; V18: float; V19: float; V20: float
    V21: float; V22: float; V23: float; V24: float; V25: float; V26: float; V27: float; V28: float
    Amount: float
    Hour: Optional[float] = 0

@app.post("/predict")
def predict(transaction: Transaction):
    try:
        df = pd.DataFrame([transaction.model_dump()])
        # feature engineering consistent with training
        if "Amount" in df.columns and "Amount_log" not in df.columns:
            df["Amount_log"] = np.log1p(df["Amount"])
        if "Hour" not in df.columns:
            df["Hour"] = 0
        expected = [f"V{i}" for i in range(1, 29)] + ["Amount", "Amount_log", "Hour"]
        df = df.reindex(columns=expected, fill_value=0.0)
        X = df.values.astype(float)
        if scaler is not None:
            try:
                X = scaler.transform(X)
            except Exception:
                pass
        probs = predict_proba_array(X)
        prob = float(probs[0])
        result = {"fraud_probability": prob, "is_fraud": bool(prob > FRAUD_THRESHOLD), "model_version": MODEL_VERSION}
        # store in transactions_log
        try:
            # ensure data directory exists
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
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
                # transaction_id not provided via API — generate simple id
                tid = f"API_{int(datetime.utcnow().timestamp()*1000)}"
                amount_val = float(df["Amount"].iloc[0]) if "Amount" in df.columns else None
                processed_at = datetime.utcnow().isoformat()
                cur.execute("""
                    INSERT INTO transactions_log (transaction_id, fraud_probability, is_fraud, amount, processed_at, model_version)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (tid, prob, int(prob > FRAUD_THRESHOLD), amount_val, processed_at, MODEL_VERSION))
                conn.commit()
        except Exception as e:
            # DB write should not block API response; log the failure
            print(f"[warn] Failed to write prediction to DB: {e}")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sample")
def sample_transaction():
    try:
        # read from transactions_log (not transactions)
        if not os.path.exists(DB_PATH):
            raise HTTPException(status_code=404, detail="No database found.")
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql_query("SELECT * FROM transactions_log ORDER BY RANDOM() LIMIT 1;", conn)
        if df.empty:
            raise HTTPException(status_code=404, detail="No transactions found.")
        return df.to_dict(orient="records")[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")), log_level="info")