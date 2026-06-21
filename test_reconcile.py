import os
import json
import sqlite3
import pytest
import pandas as pd
from reconcile import (
    _parse_currency, 
    _parse_dates, 
    _dates_within_window, 
    _amounts_match, 
    run_pipeline
)

# =====================================================================
# 1. UNIT TESTS FOR CLEANING FUNCTIONS
# =====================================================================

def test_parse_currency():
    """Test that various messy currency formats are properly cleaned to absolute floats."""
    assert _parse_currency("$1,234.50") == 1234.50
    assert _parse_currency("-$50.00") == 50.00
    assert _parse_currency("(100.00)") == 100.00
    assert _parse_currency("  500  ") == 500.00
    assert pd.isna(_parse_currency("INVALID_AMT"))


def test_parse_dates():
    """Test that diverse date strings are cleanly parsed into datetimes."""
    res1 = _parse_dates(pd.Series(["2026-06-20", "2026/06/20", "21-06-2026", "broken-date"]))
    
    assert pd.api.types.is_datetime64_any_dtype(res1)
    assert res1.iloc[0] == pd.Timestamp("2026-06-20")
    assert pd.isna(res1.iloc[3])


# =====================================================================
# 2. UNIT TESTS FOR MATCHING LOGIC
# =====================================================================

def test_weekend_settlement_delay():
    """Verify Friday/Saturday wallet txns allow up to a +3 day bank delay using Pandas Timestamps."""
    wallet_date_fri = pd.Timestamp("2026-06-19") # Friday
    bank_date_mon = pd.Timestamp("2026-06-22")   # Monday
    
    # Pass explicit pandas Timestamps as required by reconcile.py
    assert _dates_within_window(
        bank_date=bank_date_mon, 
        wallet_date=wallet_date_fri, 
        default_window_days=1, 
        weekend_forward_days=3
    ) is True

    # Tuesday Wallet Txn -> Friday Bank Txn (+3 days) -> Should Fail
    wallet_date_tue = pd.Timestamp("2026-06-16") # Tuesday
    bank_date_fri = pd.Timestamp("2026-06-19")   # Friday
    
    assert _dates_within_window(
        bank_date=bank_date_fri, 
        wallet_date=wallet_date_tue, 
        default_window_days=1, 
        weekend_forward_days=3
    ) is False


def test_fee_tolerance():
    """Verify bank amounts within the 2.5% gateway fee tolerance match successfully."""
    try:
        # Cursor likely named it 'tolerance' and expects the pure decimal (0.025)
        assert _amounts_match(bank_amount=97.50, wallet_amount=100.00, tolerance=0.025) is True
        assert _amounts_match(bank_amount=95.00, wallet_amount=100.00, tolerance=0.025) is False
    except TypeError:
        # Fallback to positional arguments using the proper decimal
        assert _amounts_match(97.50, 100.00, 0.025) is True
        assert _amounts_match(95.00, 100.00, 0.025) is False


# =====================================================================
# 3. END-TO-END INTEGRATION TEST
# =====================================================================

@pytest.fixture
def setup_mock_project(tmp_path):
    """Fixture to spin up temporary CSVs and return config dictionary for pipeline testing."""
    dir_path = tmp_path / "recon_test"
    dir_path.mkdir()
    
    config_data = {
        "thresholds": {
            "fuzzy_match_threshold": 70,
            "fee_tolerance_pct": 2.5,
            "base_date_window_days": 1,
            "weekend_forward_days": 3,
            "high_value_warning_threshold": 1000.0
        },
        "file_paths": {
            "bank_csv": str(dir_path / "bank.csv"),
            "wallet_csv": str(dir_path / "wallet.csv"),
            "sqlite_db": str(dir_path / "test_recon.db")
        },
        "column_mappings": {
            "bank": {"ref": "id", "dt": "date", "val": "amount", "desc": "description"},
            "wallet": {"id": "id", "ts": "date", "amt": "amount", "note": "description"}
        },
        "pipeline": {"generate_mock_data": False}
    }
    
    # Create matching baseline files so pipeline execution succeeds
    bank_df = pd.DataFrame([{"ref": "B001", "dt": "2026-06-20", "val": "$100.00", "desc": "Starbucks"}])
    wallet_df = pd.DataFrame([{"id": "W001", "ts": "2026-06-20", "amt": "100.00", "note": "Starbucks Store"}])
    
    bank_df.to_csv(dir_path / "bank.csv", index=False)
    wallet_df.to_csv(dir_path / "wallet.csv", index=False)
    
    return config_data, dir_path / "test_recon.db"


def test_end_to_end_pipeline(setup_mock_project):
    """Runs the whole pipeline by passing the dictionary directly as code expects."""
    config_data, db_path = setup_mock_project
    
    # Pass config dictionary directly to bypass string parsing limits
    run_pipeline(config=config_data)
    
    assert os.path.exists(db_path)
    
    conn = sqlite3.connect(db_path)
    matched_df = pd.read_sql("SELECT * FROM matched_transactions", conn)
    exec_summary_df = pd.read_sql("SELECT * FROM v_executive_summary", conn)
    conn.close()
    
    assert len(matched_df) == 1
    assert matched_df.iloc[0]["bank_id"] == "B001"
    assert exec_summary_df.iloc[0]["total_matched_pairs"] == 1