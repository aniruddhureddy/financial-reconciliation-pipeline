import streamlit as pd
import streamlit as st
import os
import json
import sqlite3
import pandas as pd
from reconcile import run_pipeline

st.set_page_config(page_title="Financial Reconciliation Pipeline", page_icon="💰", layout="wide")

st.title("💰 Enterprise Financial Reconciliation System")
st.markdown("Upload your raw transaction logs from your Bank and Payment Gateway to automatically match records.")

# --- SIDEBAR: Configuration Thresholds ---
st.sidebar.header("Pipeline Settings")
fuzzy_threshold = st.sidebar.slider("Fuzzy Match Threshold (Description)", 50, 100, 70)
fee_tolerance = st.sidebar.slider("Gateway Fee Tolerance (%)", 0.0, 5.0, 2.5, step=0.1)

# --- STEP 1: File Uploads ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("🏦 1. Bank Statement")
    bank_file = st.file_uploader("Upload Bank CSV", type=["csv"], key="bank")

with col2:
    st.subheader("💳 2. Payment Gateway / Wallet Log")
    wallet_file = st.file_uploader("Upload Wallet CSV", type=["csv"], key="wallet")

# --- STEP 2: Process Execution ---
if bank_file and wallet_file:
    st.success("Both files uploaded successfully!")
    
    if st.button("🚀 Run Reconciliation Pipeline", type="primary"):
        with st.spinner("Processing data, normalizing currencies, and executing matching algorithms..."):
            
            # Save uploaded files temporarily to disk so reconcile.py can read them
            with open("temp_bank.csv", "wb") as f:
                f.write(bank_file.getbuffer())
            with open("temp_wallet.csv", "wb") as f:
                f.write(wallet_file.getbuffer())
                
            # Build dynamic config dictionary
            runtime_config = {
                "thresholds": {
                    "fuzzy_match_threshold": fuzzy_threshold,
                    "fee_tolerance_pct": fee_tolerance,
                    "base_date_window_days": 1,
                    "weekend_forward_days": 3,
                    "high_value_warning_threshold": 1000.0
                },
                "file_paths": {
                    "bank_csv": "temp_bank.csv",
                    "wallet_csv": "temp_wallet.csv",
                    "sqlite_db": "reconciliation_results.db"
                },
              "column_mappings": {
                    "bank": {"id": "id", "date": "date", "amount": "amount", "description": "description"},
                    "wallet": {"id": "id", "date": "date", "amount": "amount", "description": "description"}
                },
          "pipeline": {"generate_mock_data": False} # Force real data mode
            }
            
            try:
                # Execute your backend code!
                run_pipeline(config=runtime_config)
                
               # --- STEP 3: Display Live Results ---
                st.balloons()
                st.markdown("---")
                st.header("📊 Execution Results")
                
                # Pull metrics by directly counting the tables (bulletproof method)
                conn = sqlite3.connect("reconciliation_results.db")
                matched_count = pd.read_sql("SELECT COUNT(*) as cnt FROM matched_transactions", conn).iloc[0]['cnt']
                unmatched_bank_count = pd.read_sql("SELECT COUNT(*) as cnt FROM unmatched_bank", conn).iloc[0]['cnt']
                unmatched_wallet_count = pd.read_sql("SELECT COUNT(*) as cnt FROM unmatched_wallet", conn).iloc[0]['cnt']
                conn.close()
                
                # Calculate the match rate safely
                total_bank_records = matched_count + unmatched_bank_count
                bank_match_rate = (matched_count / total_bank_records * 100) if total_bank_records > 0 else 0
                
                m_col1, m_col2, m_col3, m_col4 = st.columns(4)
                m_col1.metric("Total Matched Pairs", int(matched_count))
                m_col2.metric("Unmatched Bank Records", int(unmatched_bank_count))
                m_col3.metric("Unmatched Wallet Records", int(unmatched_wallet_count))
                m_col4.metric("Bank Match Rate", f"{bank_match_rate:.1f}%")
                
                st.success("Database `reconciliation_results.db` updated successfully. You can now refresh Tableau!")
                
            finally:
                # Clean up temporary source files
                if os.path.exists("temp_bank.csv"): os.remove("temp_bank.csv")
                if os.path.exists("temp_wallet.csv"): os.remove("temp_wallet.csv")
else:
    st.info("Please upload both CSV files to unlock the reconciliation engine.")