# realtime_dashboard.py — Streamlit live dashboard (robust & container-ready)
import os
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
from typing import Tuple, Optional

# Config
DB_PATH = os.getenv("DB_PATH", "/app/data/fraud_detection.db")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "5"))
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.001"))

st.set_page_config(page_title="Real-Time Credit Card Fraud Detection", page_icon="💳", layout="wide")
st.title("💳 Real-Time Credit Card Fraud Detection Dashboard")
st.markdown("<br>", unsafe_allow_html=True)

# Auto-refresh
st_autorefresh(interval=REFRESH_SECONDS * 1000, key="auto_refresh_key")

# -------------------------
# Helpers
# -------------------------
def safe_connect(db_path: str) -> Tuple[Optional[sqlite3.Connection], Optional[str]]:
    """Return connection or an error message (do not call st.* here)."""
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        return conn, None
    except Exception as e:
        return None, str(e)

@st.cache_data(ttl=5)
def load_data_from_db(db_path: str) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Load and normalize data from transactions_log.
    Return (df, error_message). This function should avoid UI side effects.
    """
    conn, conn_err = safe_connect(db_path)
    if conn is None:
        return pd.DataFrame(), f"DB connect error: {conn_err}"

    try:
        df = pd.read_sql_query("SELECT * FROM transactions_log ORDER BY id DESC LIMIT 20000;", conn)
        conn.close()

        if df.empty:
            return pd.DataFrame(), None

        df = df.sort_values("id").reset_index(drop=True)
        df.columns = [c.lower() for c in df.columns]
        return df, None

    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return pd.DataFrame(), f"Query/load error: {str(e)}"

def safe_plot(plot_func, *args, **kwargs):
    """
    Run a plotting function that calls st.plotly_chart or similar.
    If it errors, display an error message in place of that plot but keep app running.
    """
    try:
        plot_func(*args, **kwargs)
    except Exception as e:
        st.error(f"Chart failed to render: {str(e)[:200]}")

def get_sample_df():
    """Small fallback dataset to show when DB is empty or failing."""
    times = pd.date_range(end=pd.Timestamp.now(), periods=20, freq="T")
    return pd.DataFrame({
        "index": list(range(20)),
        "fraud_probability": [0.05 * (i % 5) for i in range(20)],
        "amount": [10 + i * 5 for i in range(20)]
    })

# -------------------------
# Load data (safe)
# -------------------------
df, load_err = load_data_from_db(DB_PATH)

# Show DB status prominently but do NOT stop rendering the rest of the UI
if load_err:
    st.error(f"Database issue: {load_err}")
elif df.empty:
    st.warning("No transactions found in transactions_log — showing fallback/sample visuals.")

# Derived/guarded values
if df.empty:
    # fallback small dataset so layout and titles render
    fallback_df = get_sample_df()
    display_df = fallback_df.copy()
else:
    display_df = df.copy()

total = len(display_df)

# compute frauds safely
if "is_fraud" in display_df.columns:
    frauds = display_df[display_df["is_fraud"] == 1]
elif "fraud_probability" in display_df.columns:
    frauds = display_df[display_df["fraud_probability"] > FRAUD_THRESHOLD]
else:
    frauds = pd.DataFrame()

fraud_rate = (len(frauds) / total * 100) if total > 0 else 0.0

# Top-level metrics (always render)
st.markdown("### 📊 Live Fraud Risk Overview")
colg, colk = st.columns([2, 3])

with colg:
    try:
        gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=fraud_rate,
            title={"text": "Live Fraud Rate (%)"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "crimson" if fraud_rate > 10 else "green"},
                "steps": [
                    {"range": [0, 20], "color": "green"},
                    {"range": [20, 50], "color": "yellow"},
                    {"range": [50, 100], "color": "red"}
                ]
            }
        ))
        gauge.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(gauge, use_container_width=True)
    except Exception as e:
        st.error(f"Gauge failed: {str(e)[:200]}")

with colk:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Transactions", total)
    col2.metric("Fraudulent Transactions", len(frauds))
    col3.metric("Fraud Rate (%)", f"{fraud_rate:.2f}")

st.markdown("---")

# Safe creation of a time bucket column
try:
    # If original index isn't informative, fallback to range index
    if "processed_at" in display_df.columns:
        # try to coerce processed_at to datetime for meaningful buckets
        try:
            display_df["processed_at"] = pd.to_datetime(display_df["processed_at"], errors="coerce")
            # use time-based resampling if we have datetimes
            if display_df["processed_at"].notna().any():
                display_df = display_df.sort_values("processed_at").reset_index(drop=True)
                display_df["time_bucket"] = pd.cut(
                    display_df.index, bins=min(50, max(2, len(display_df))), labels=False
                )
            else:
                display_df["time_bucket"] = pd.cut(display_df.index, bins=min(50, max(2, len(display_df))), labels=False)
        except Exception:
            display_df["time_bucket"] = pd.cut(display_df.index, bins=min(50, max(2, len(display_df))), labels=False)
    else:
        display_df["time_bucket"] = pd.cut(display_df.index, bins=min(50, max(2, len(display_df))), labels=False)
except Exception:
    display_df["time_bucket"] = display_df.index  # last resort

# 1. Volume Over Time
st.markdown("#### 🔢 Transaction Volume Over Time")
try:
    volume = display_df.groupby("time_bucket").size().reset_index(name="count")
    fig_vol = px.bar(volume, x="time_bucket", y="count", title="Transactions per time bucket")
    fig_vol.update_layout(height=300)
    st.plotly_chart(fig_vol, use_container_width=True)
except Exception as e:
    st.error(f"Volume chart failed: {str(e)[:200]}")

# 2. Fraud Probability Distribution
if "fraud_probability" in display_df.columns:
    st.markdown("#### 📈 Fraud Probability Distribution")
    try:
        fig_hist = px.histogram(
            display_df, x="fraud_probability", nbins=30,
            title="Fraud Probability Distribution"
        )
        fig_hist.update_layout(height=300)
        st.plotly_chart(fig_hist, use_container_width=True)
    except Exception as e:
        st.error(f"Histogram failed: {str(e)[:200]}")

# 3. Fraud Probability Trend
if "fraud_probability" in display_df.columns:
    st.markdown("#### ⏱ Fraud Probability Trend")
    try:
        fig_line = px.line(display_df.reset_index(), x="index", y="fraud_probability", title="Fraud Probability Over Time")
        fig_line.update_layout(height=300)
        st.plotly_chart(fig_line, use_container_width=True)
    except Exception as e:
        st.error(f"Trend chart failed: {str(e)[:200]}")

# 4. Amount vs Fraud Probability
if "amount" in display_df.columns and "fraud_probability" in display_df.columns:
    st.markdown("#### 💰 Amount vs Fraud Probability (Anomaly Map)")
    try:
        color_col = "is_fraud" if "is_fraud" in display_df.columns else None
        fig_scatter = px.scatter(display_df, x="amount", y="fraud_probability", color=color_col, title="Amount vs Fraud Probability", size_max=8)
        fig_scatter.update_layout(height=400)
        st.plotly_chart(fig_scatter, use_container_width=True)
    except Exception as e:
        st.error(f"Scatter failed: {str(e)[:200]}")

# 5. Heatmap (guarded)
if "fraud_probability" in display_df.columns:
    st.markdown("#### 🔥 Fraud Intensity Heatmap")
    try:
        # ensure is_fraud exists for grouping; if not, create a placeholder
        heat_grp_cols = ["time_bucket"] + (["is_fraud"] if "is_fraud" in display_df.columns else [])
        heat_df = display_df.groupby(heat_grp_cols, observed=True)["fraud_probability"].mean().reset_index()
        # If is_fraud missing, create a dummy column for plotting
        if "is_fraud" not in heat_df.columns:
            heat_df["is_fraud"] = 0
        fig_heat = px.density_heatmap(heat_df, x="time_bucket", y="is_fraud", z="fraud_probability", title="Avg Fraud Prob by Time Bucket / Fraud Flag")
        fig_heat.update_layout(height=350)
        st.plotly_chart(fig_heat, use_container_width=True)
    except Exception as e:
        st.error(f"Heatmap failed: {str(e)[:200]}")

# 6. Top suspicious transactions (safe sort and safe columns)
st.markdown("#### 🚨 Top Suspicious Transactions (by probability)")
try:
    sort_col = "fraud_probability" if "fraud_probability" in display_df.columns else ("amount" if "amount" in display_df.columns else display_df.columns[0])
    top_suspicious = display_df.sort_values(sort_col, ascending=False).head(10)
    display_cols = [c for c in ["transaction_id", "fraud_probability", "amount", "processed_at"] if c in top_suspicious.columns]
    if not display_cols:
        display_cols = list(top_suspicious.columns[:5])
    st.dataframe(top_suspicious[display_cols].fillna("-"))
except Exception as e:
    st.error(f"Top suspicious table failed: {str(e)[:200]}")

# 10. Summary table
st.markdown("#### 📋 Summary Table")
try:
    summary = {
        "Total": [total],
        "Frauds": [len(frauds)],
        "Fraud Rate (%)": [f"{fraud_rate:.2f}"],
    }
    st.table(pd.DataFrame(summary))
except Exception as e:
    st.error(f"Summary table failed: {str(e)[:200]}")