import streamlit as st

st.set_page_config(page_title="GitPulse", page_icon="🔍", layout="wide")

st.title("🔍 GitPulse")
st.subheader("GitHub Repository Health Intelligence Platform")

st.info("Pipeline is being set up. Dashboard coming soon!")

col1, col2, col3 = st.columns(3)
col1.metric("Repos Monitored", "0", "Starting up...")
col2.metric("Abandoned Risk Alerts", "0", "Starting up...")
col3.metric("Viral Candidates", "0", "Starting up...")
