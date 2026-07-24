import os

import pandas as pd
import psycopg2
import streamlit as st

st.set_page_config(page_title="GitPulse", page_icon="🔍", layout="wide")
st.title("🔍 GitPulse")
st.subheader("GitHub Repository Health Intelligence Platform")

db_params = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": os.environ.get("DB_PORT", "5432"),
    "dbname": os.environ.get("DB_NAME", "gitpulse"),
    "user": os.environ.get("DB_USER", "gitpulse"),
    "password": os.environ.get("DB_PASSWORD", "gitpulse"),
}


def run_query(query: str) -> pd.DataFrame:
    with psycopg2.connect(**db_params) as conn:
        return pd.read_sql_query(query, conn)


st.header("Batch")
try:
    batch_df = run_query(
        """
        SELECT repo_name, week_start, activity_score
        FROM repo_weekly_features
        ORDER BY week_start DESC, activity_score DESC
        LIMIT 200
        """
    )
    st.dataframe(batch_df, use_container_width=True)

    top_activity = (
        batch_df.groupby("repo_name", as_index=False)["activity_score"]
        .sum()
        .sort_values("activity_score", ascending=False)
        .head(10)
    )
    st.subheader("Top repos by activity score")
    st.bar_chart(top_activity.set_index("repo_name"))
except Exception as exc:
    st.warning(f"Batch section unavailable: {exc}")

st.header("Live")
if st.button("Refresh Live Feed"):
    st.rerun()

try:
    live_df = run_query(
        """
        SELECT id, event_type, repo_name, created_at
        FROM github_events_raw
        ORDER BY created_at DESC
        LIMIT 50
        """
    )
    st.dataframe(live_df, use_container_width=True)
except Exception as exc:
    st.warning(f"Live section unavailable: {exc}")
