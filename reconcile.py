"""
Multi-Source Financial Reconciliation Tool
==========================================

Normalizes transaction exports from two differently-shaped sources (e.g., bank
statement and wallet/UPI export), matches records by amount, date window, and
fuzzy description similarity, and persists results to SQLite for Tableau.

Configuration is externalized to ``config.json`` (thresholds, file paths, and
per-source column mappings). Run with::

    python reconcile.py
    python reconcile.py --config path/to/config.json

Dependencies: pandas, rapidfuzz, sqlite3 (stdlib)
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Constants (schema contract — not user-configurable)
# ---------------------------------------------------------------------------

NORMALIZED_COLUMNS = ["id", "date", "amount", "description", "source"]
REQUIRED_INTERNAL_FIELDS = {"id", "date", "amount", "description"}
DEFAULT_CONFIG_PATH = "config.json"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """
    Load and validate ``config.json``.

    Raises:
        FileNotFoundError: Config file does not exist.
        ValueError: Required sections or keys are missing.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path.resolve()}")

    with config_path.open(encoding="utf-8") as handle:
        config = json.load(handle)

    _validate_config(config)
    logger.info("Loaded configuration from %s", config_path.resolve())
    return config


def _validate_config(config: dict[str, Any]) -> None:
    """Ensure the config contains all required sections and mapping targets."""
    required_sections = ("thresholds", "file_paths", "column_mappings")
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Config missing required section: '{section}'")

    threshold_keys = (
        "fuzzy_match_threshold",
        "fee_tolerance_pct",
        "base_date_window_days",
    )
    for key in threshold_keys:
        if key not in config["thresholds"]:
            raise ValueError(f"Config thresholds missing required key: '{key}'")

    path_keys = ("bank_csv", "wallet_csv", "sqlite_db")
    for key in path_keys:
        if key not in config["file_paths"]:
            raise ValueError(f"Config file_paths missing required key: '{key}'")

    for source in ("bank", "wallet"):
        mapping = config["column_mappings"].get(source)
        if not mapping:
            raise ValueError(f"Config column_mappings missing '{source}' mapping")

        targets = set(mapping.values())
        missing_targets = REQUIRED_INTERNAL_FIELDS - targets
        if missing_targets:
            raise ValueError(
                f"Config column_mappings.{source} must map to internal fields "
                f"{sorted(REQUIRED_INTERNAL_FIELDS)}; missing: {sorted(missing_targets)}"
            )


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logging format for pipeline milestones."""
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
        force=True,
    )


def _fee_rate_from_pct(fee_tolerance_pct: float) -> float:
    """Convert a percentage (e.g. 2.5) to a decimal rate (0.025)."""
    return fee_tolerance_pct / 100.0


# ---------------------------------------------------------------------------
# 1. Synthetic Data Generation
# ---------------------------------------------------------------------------

def generate_synthetic_data(
    bank_path: str | Path,
    wallet_path: str | Path,
    config: dict[str, Any],
) -> tuple[Path, Path]:
    """
    Create two sample CSV files with intentional match / mismatch patterns.

    Column headers are derived from ``config['column_mappings']`` so the mock
    files align with the configured source schemas.

    Returns paths to the generated bank and wallet CSV files.
    """
    bank_path = Path(bank_path)
    wallet_path = Path(wallet_path)

    bank_cols = list(config["column_mappings"]["bank"].keys())
    wallet_cols = list(config["column_mappings"]["wallet"].keys())

    # (txn_ref, post_date, value, narrative, balance)
    bank_rows: list[tuple] = [
        ("BNK001", "2025-06-01", -450.00, "Starbucks Coffee", 9550.00),
        ("BNK002", "2025-06-02", -1200.50, "Amazon India Marketplace", 8350.50),
        ("BNK003", "2025-06-03", 5000.00, "Salary Credit ACME Corp", 13350.50),
        ("BNK004", "2025-06-04", -89.99, "Netflix Subscription", 13260.51),
        ("BNK005", "2025-06-05", -2500.00, "Rent Payment June", 10760.51),
        ("BNK006", "2025-06-06", -350.00, "Swiggy Order 8821", 10410.51),
        ("BNK007", "2025-06-08", -999.00, "Flipkart Online Shopping", 9411.51),
        ("BNK008", "2025-06-10", -75.00, "Uber Ride Mumbai", 9336.51),
        ("BNK009", "2025-06-12", -1500.00, "Electricity Bill MSEDCL", 7836.51),
        ("BNK010", "2025-06-15", -200.00, "ATM Cash Withdrawal", 7636.51),
        ("BNK011", "2025-06-18", -45.00, "Bank Service Charge", 7591.51),
        ("BNK012", "2025-06-20", 150.00, "Interest Credit Q2", 7741.51),
    ]

    # (id, timestamp, txn_amount, merchant_notes, status)
    wallet_rows: list[tuple] = [
        ("WLT001", "2025-06-01 08:32:15", -450.00, "Starbucks Coffee", "SUCCESS"),
        ("WLT002", "2025-06-02 14:20:00", -1200.50, "Amazon India Marketplace", "SUCCESS"),
        ("WLT003", "2025-06-03 09:00:01", 5000.00, "Salary Credit ACME Corp", "SUCCESS"),
        ("WLT004", "2025-06-04 00:01:00", -89.99, "Netflix Subscription", "SUCCESS"),
        ("WLT005", "2025-06-05 11:30:00", -2500.00, "Rent Payment June", "SUCCESS"),
        ("WLT006", "2025-06-07 19:45:00", -350.00, "Swiggy Order #8821", "SUCCESS"),
        ("WLT007", "2025-06-07 10:00:00", -999.00, "Flipkart Online Shopping Payment", "SUCCESS"),
        ("WLT008", "2025-06-10 22:15:00", -75.00, "Uber Trip Mumbai", "SUCCESS"),
        ("WLT009", "2025-06-13 08:00:00", -1500.00, "MSEDCL Electricity Bill", "SUCCESS"),
        ("WLT010", "2025-06-16 12:00:00", -99.00, "Spotify Premium", "SUCCESS"),
        ("WLT011", "2025-06-19 18:30:00", -500.00, "Zomato Gold", "SUCCESS"),
        ("WLT012", "2025-06-21 07:00:00", -30.00, "Google Play", "FAILED"),
    ]

    # Pad rows if mapping has fewer columns than mock tuples (e.g. no balance/status)
    bank_df = pd.DataFrame(
        [row[: len(bank_cols)] for row in bank_rows],
        columns=bank_cols,
    )
    wallet_df = pd.DataFrame(
        [row[: len(wallet_cols)] for row in wallet_rows],
        columns=wallet_cols,
    )

    bank_df.to_csv(bank_path, index=False)
    wallet_df.to_csv(wallet_path, index=False)

    logger.info("Generated bank export  -> %s (%d rows)", bank_path.resolve(), len(bank_df))
    logger.info(
        "Generated wallet export -> %s (%d rows)", wallet_path.resolve(), len(wallet_df)
    )

    return bank_path, wallet_path


# ---------------------------------------------------------------------------
# 2. Data Normalization & Validation
# ---------------------------------------------------------------------------

_CURRENCY_SYMBOLS = re.compile(r"[$€£₹¥]")


def _clean_description(text: object) -> str:
    """Lowercase, strip whitespace, and collapse repeated spaces."""
    if pd.isna(text):
        return ""
    cleaned = str(text).lower().strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _parse_currency(value: object) -> float | None:
    """
    Parse a currency value into a clean absolute float.

    Handles numeric types, strings with symbols/commas (``$1,234.50``,
    ``-$50.00``), and accounting-style parentheses for negatives.
    Returns ``None`` when parsing fails.
    """
    if pd.isna(value):
        return None

    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return abs(float(value))

    text = str(value).strip()
    if not text:
        return None

    text = _CURRENCY_SYMBOLS.sub("", text)
    text = text.replace(",", "").replace(" ", "")

    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1]

    if text.startswith("-"):
        text = text[1:]

    if not text or text == ".":
        return None

    try:
        return abs(float(text))
    except ValueError:
        return None


def _parse_dates(series: pd.Series) -> pd.Series:
    """Parse dates flexibly; unparseable values become ``NaT``."""
    return pd.to_datetime(series, errors="coerce")


def _validate_and_clean(
    df: pd.DataFrame,
    source_name: str,
    raw_row_count: int,
) -> pd.DataFrame:
    """
    Drop rows with missing/invalid amount or date and log diagnostics.

    Rows with unparseable amounts or dates are flagged via ERROR logs; all
    invalid rows are dropped with a WARNING summarizing the count.
    """
    result = df.copy()

    missing_amount = result["amount"].isna()
    missing_date = result["date"].isna()
    missing_id = result["id"].isna() | (result["id"].astype(str).str.strip() == "")

    bad_amount_count = int(missing_amount.sum())
    bad_date_count = int(missing_date.sum())

    if bad_amount_count:
        sample_ids = result.loc[missing_amount, "id"].astype(str).head(5).tolist()
        logger.error(
            "%s: %d row(s) failed amount parsing (sample ids: %s)",
            source_name,
            bad_amount_count,
            sample_ids,
        )

    if bad_date_count:
        sample_ids = result.loc[missing_date, "id"].astype(str).head(5).tolist()
        logger.error(
            "%s: %d row(s) failed date parsing (sample ids: %s)",
            source_name,
            bad_date_count,
            sample_ids,
        )

    if int(missing_id.sum()):
        logger.warning(
            "%s: %d row(s) have missing id; synthetic ids will not be assigned",
            source_name,
            int(missing_id.sum()),
        )

    invalid_mask = missing_amount | missing_date
    dropped = int(invalid_mask.sum())

    if dropped:
        logger.warning(
            "%s: dropping %d of %d row(s) with missing/invalid amount or date",
            source_name,
            dropped,
            raw_row_count,
        )

    cleaned = result.loc[~invalid_mask].reset_index(drop=True)
    return cleaned[NORMALIZED_COLUMNS]


def normalize_source(
    raw_df: pd.DataFrame,
    column_mapping: dict[str, str],
    source_name: str,
) -> pd.DataFrame:
    """
    Map raw CSV columns to the internal schema and apply cleansing rules.

    ``column_mapping`` maps raw header names to internal names, e.g.
    ``{"txn_ref": "id", "post_date": "date", ...}``.
    """
    raw_row_count = len(raw_df)

    missing_cols = set(column_mapping.keys()) - set(raw_df.columns)
    if missing_cols:
        raise ValueError(
            f"{source_name}: CSV is missing mapped column(s): {sorted(missing_cols)}. "
            f"Available columns: {list(raw_df.columns)}"
        )

    working = raw_df[list(column_mapping.keys())].rename(columns=column_mapping)

    working["id"] = working["id"].astype(str).str.strip()
    working["amount"] = working["amount"].map(_parse_currency)
    working["date"] = _parse_dates(working["date"])
    working["description"] = working["description"].map(_clean_description)
    working["source"] = source_name

    return _validate_and_clean(working, source_name, raw_row_count)


def load_and_normalize(
    bank_path: str | Path,
    wallet_path: str | Path,
    column_mappings: dict[str, dict[str, str]],
) -> pd.DataFrame:
    """
    Read both CSVs, normalize via config-driven mappings, and combine.
    """
    bank_df = pd.read_csv(bank_path)
    wallet_df = pd.read_csv(wallet_path)

    bank_norm = normalize_source(bank_df, column_mappings["bank"], "Bank")
    wallet_norm = normalize_source(wallet_df, column_mappings["wallet"], "Wallet")

    logger.info("Loaded %d bank records from %s", len(bank_norm), bank_path)
    logger.info("Loaded %d wallet records from %s", len(wallet_norm), wallet_path)

    return pd.concat([bank_norm, wallet_norm], ignore_index=True)


# ---------------------------------------------------------------------------
# 3. Reconciliation / Matching Logic
# ---------------------------------------------------------------------------

def _dates_within_window(
    bank_date: pd.Timestamp,
    wallet_date: pd.Timestamp,
    default_window_days: int,
    weekend_forward_days: int,
) -> bool:
    """
    Return True when bank and wallet dates satisfy the matching window.

    Standard rule: dates within +/- default_window_days.
    Weekend settlement: when the wallet transaction falls on Friday or
    Saturday, allow the bank posting to lag up to weekend_forward_days
    calendar days after the wallet date (still within default_window_days before).
    """
    if pd.isna(bank_date) or pd.isna(wallet_date):
        return False

    bank_day = bank_date.normalize()
    wallet_day = wallet_date.normalize()
    delta_days = (bank_day - wallet_day).days

    wallet_weekday = wallet_day.weekday()
    if wallet_weekday in (4, 5):
        return -default_window_days <= delta_days <= weekend_forward_days

    return abs(delta_days) <= default_window_days


def _amounts_match(
    bank_amount: float,
    wallet_amount: float,
    fee_rate: float,
) -> bool:
    """
    Return True when bank and wallet amounts align exactly or within gateway
    fee tolerance (bank amount is within fee_rate below wallet amount).
    """
    if bank_amount == wallet_amount:
        return True

    lower_bound = wallet_amount * (1.0 - fee_rate)
    return lower_bound <= bank_amount < wallet_amount


def _amount_difference(wallet_amount: float, bank_amount: float) -> float:
    """Fee delta: wallet amount minus bank amount (0 for exact matches)."""
    return round(wallet_amount - bank_amount, 2)


def _fuzzy_score(desc_a: str, desc_b: str) -> float:
    """Token-sort ratio handles word reordering and partial overlaps well."""
    return float(fuzz.token_sort_ratio(desc_a, desc_b))


def _candidate_rank(
    bank_amount: float,
    wallet_amount: float,
    fuzzy_score: float,
) -> tuple[int, float, float]:
    """Rank candidates: exact amount > fuzzy score > smallest fee delta."""
    exact = 1 if bank_amount == wallet_amount else 0
    fee_delta = _amount_difference(wallet_amount, bank_amount)
    return (exact, fuzzy_score, -fee_delta)


def reconcile_transactions(
    normalized_df: pd.DataFrame,
    thresholds: dict[str, float | int],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Match Bank transactions to Wallet transactions (1-to-1).

    Thresholds are read from ``config['thresholds']`` (passed as ``thresholds``).
    """
    fuzzy_threshold = float(thresholds["fuzzy_match_threshold"])
    date_window_days = int(thresholds["base_date_window_days"])
    weekend_forward_days = int(thresholds.get("weekend_forward_days", 3))
    fee_rate = _fee_rate_from_pct(float(thresholds["fee_tolerance_pct"]))
    high_value_threshold = float(thresholds.get("high_value_warning_threshold", 1000.0))

    bank_df = (
        normalized_df[normalized_df["source"] == "Bank"]
        .copy()
        .reset_index(drop=True)
    )
    wallet_df = (
        normalized_df[normalized_df["source"] == "Wallet"]
        .copy()
        .reset_index(drop=True)
    )

    matched_wallet_indices: set[int] = set()
    matches: list[dict] = []

    for _, bank_row in bank_df.iterrows():
        best_wallet_idx: int | None = None
        best_rank: tuple[int, float, float] | None = None

        for wallet_idx, wallet_row in wallet_df.iterrows():
            if wallet_idx in matched_wallet_indices:
                continue

            if not _amounts_match(bank_row["amount"], wallet_row["amount"], fee_rate):
                continue

            if not _dates_within_window(
                bank_row["date"],
                wallet_row["date"],
                date_window_days,
                weekend_forward_days,
            ):
                continue

            score = _fuzzy_score(bank_row["description"], wallet_row["description"])
            if score <= fuzzy_threshold:
                continue

            rank = _candidate_rank(bank_row["amount"], wallet_row["amount"], score)
            if best_rank is None or rank > best_rank:
                best_rank = rank
                best_wallet_idx = wallet_idx

        if best_wallet_idx is not None:
            wallet_row = wallet_df.loc[best_wallet_idx]
            matched_wallet_indices.add(best_wallet_idx)

            match_date = min(bank_row["date"], wallet_row["date"])
            bank_amount = float(bank_row["amount"])
            wallet_amount = float(wallet_row["amount"])

            matches.append(
                {
                    "bank_id": bank_row["id"],
                    "wallet_id": wallet_row["id"],
                    "date": match_date,
                    "bank_amount": bank_amount,
                    "wallet_amount": wallet_amount,
                    "amount_difference": _amount_difference(wallet_amount, bank_amount),
                    "bank_description": bank_row["description"],
                    "wallet_description": wallet_row["description"],
                    "fuzzy_score": round(best_rank[1], 2),
                }
            )

    matched_df = pd.DataFrame(matches)

    matched_bank_ids = set(matched_df["bank_id"]) if not matched_df.empty else set()
    matched_wallet_ids = set(matched_df["wallet_id"]) if not matched_df.empty else set()

    unmatched_bank_df = bank_df[~bank_df["id"].isin(matched_bank_ids)].reset_index(
        drop=True
    )
    unmatched_wallet_df = wallet_df[
        ~wallet_df["id"].isin(matched_wallet_ids)
    ].reset_index(drop=True)

    logger.info("Reconciliation complete: %d matched pairs", len(matched_df))
    if len(unmatched_bank_df):
        logger.info("Unmatched bank records: %d", len(unmatched_bank_df))
    if len(unmatched_wallet_df):
        logger.info("Unmatched wallet records: %d", len(unmatched_wallet_df))

    high_value_wallet = unmatched_wallet_df[
        unmatched_wallet_df["amount"] > high_value_threshold
    ]
    if not high_value_wallet.empty:
        logger.warning(
            "Found %d unmatched wallet records over $%.0f (total $%.2f)",
            len(high_value_wallet),
            high_value_threshold,
            high_value_wallet["amount"].sum(),
        )

    return matched_df, unmatched_bank_df, unmatched_wallet_df


# ---------------------------------------------------------------------------
# 4. Export to SQLite
# ---------------------------------------------------------------------------

EXECUTIVE_SUMMARY_VIEW = """
CREATE VIEW v_executive_summary AS
WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM matched_transactions)
        + (SELECT COUNT(*) FROM unmatched_bank) AS bank_records,
        (SELECT COUNT(*) FROM matched_transactions)
        + (SELECT COUNT(*) FROM unmatched_wallet) AS wallet_records,
        (SELECT COUNT(*) FROM matched_transactions) AS matched_pairs
),
value_totals AS (
    SELECT
        COALESCE((SELECT SUM(wallet_amount) FROM matched_transactions), 0)
            AS total_matched_value,
        COALESCE((SELECT SUM(amount) FROM unmatched_bank), 0)
            AS total_unmatched_bank_value,
        COALESCE((SELECT SUM(amount) FROM unmatched_wallet), 0)
            AS total_unmatched_wallet_value
)
SELECT
    c.bank_records + c.wallet_records AS total_records,
    c.matched_pairs AS total_matched_pairs,
    ROUND(
        100.0 * c.matched_pairs / NULLIF(c.bank_records, 0),
        2
    ) AS bank_match_rate_pct,
    ROUND(
        100.0 * c.matched_pairs / NULLIF(c.wallet_records, 0),
        2
    ) AS wallet_match_rate_pct,
    ROUND(
        100.0 * c.matched_pairs
        / NULLIF((c.bank_records + c.wallet_records) / 2.0, 0),
        2
    ) AS overall_match_rate_pct,
    v.total_matched_value,
    v.total_unmatched_bank_value,
    v.total_unmatched_wallet_value
FROM counts c
CROSS JOIN value_totals v;
"""

DAILY_TRENDS_VIEW = """
CREATE VIEW v_daily_trends AS
SELECT
    txn_date,
    SUM(matched_count) AS matched_count,
    SUM(unmatched_bank_count) AS unmatched_bank_count,
    SUM(unmatched_wallet_count) AS unmatched_wallet_count,
    SUM(matched_volume) AS matched_volume,
    SUM(unmatched_bank_volume) AS unmatched_bank_volume,
    SUM(unmatched_wallet_volume) AS unmatched_wallet_volume
FROM (
    SELECT
        date AS txn_date,
        COUNT(*) AS matched_count,
        0 AS unmatched_bank_count,
        0 AS unmatched_wallet_count,
        COALESCE(SUM(wallet_amount), 0) AS matched_volume,
        0.0 AS unmatched_bank_volume,
        0.0 AS unmatched_wallet_volume
    FROM matched_transactions
    GROUP BY date

    UNION ALL

    SELECT
        date AS txn_date,
        0,
        COUNT(*),
        0,
        0.0,
        COALESCE(SUM(amount), 0),
        0.0
    FROM unmatched_bank
    GROUP BY date

    UNION ALL

    SELECT
        date AS txn_date,
        0,
        0,
        COUNT(*),
        0.0,
        0.0,
        COALESCE(SUM(amount), 0)
    FROM unmatched_wallet
    GROUP BY date
)
GROUP BY txn_date
ORDER BY txn_date;
"""


def _create_tableau_views(conn: sqlite3.Connection) -> None:
    """Create Tableau-ready summary views inside the SQLite database."""
    conn.execute("DROP VIEW IF EXISTS v_executive_summary")
    conn.execute("DROP VIEW IF EXISTS v_daily_trends")
    conn.executescript(EXECUTIVE_SUMMARY_VIEW)
    conn.executescript(DAILY_TRENDS_VIEW)
    logger.info("Created Tableau views: v_executive_summary, v_daily_trends")


def export_to_sqlite(
    matched_df: pd.DataFrame,
    unmatched_bank_df: pd.DataFrame,
    unmatched_wallet_df: pd.DataFrame,
    db_path: str | Path,
) -> Path:
    """
    Write reconciliation results to SQLite tables and Tableau summary views.
    """
    db_path = Path(db_path)

    matched_out = matched_df.copy()
    if not matched_out.empty:
        matched_out["date"] = matched_out["date"].dt.strftime("%Y-%m-%d")

    unmatched_bank_out = unmatched_bank_df.copy()
    if not unmatched_bank_out.empty:
        unmatched_bank_out["date"] = unmatched_bank_out["date"].dt.strftime(
            "%Y-%m-%d"
        )

    unmatched_wallet_out = unmatched_wallet_df.copy()
    if not unmatched_wallet_out.empty:
        unmatched_wallet_out["date"] = unmatched_wallet_out["date"].dt.strftime(
            "%Y-%m-%d"
        )

    with sqlite3.connect(db_path) as conn:
        matched_out.to_sql("matched_transactions", conn, if_exists="replace", index=False)
        unmatched_bank_out.to_sql("unmatched_bank", conn, if_exists="replace", index=False)
        unmatched_wallet_out.to_sql(
            "unmatched_wallet", conn, if_exists="replace", index=False
        )
        _create_tableau_views(conn)

    logger.info("SQLite database written -> %s", db_path.resolve())
    logger.info("Tables: matched_transactions, unmatched_bank, unmatched_wallet")
    logger.info("Views:  v_executive_summary, v_daily_trends")

    return db_path


# ---------------------------------------------------------------------------
# Summary / Reporting
# ---------------------------------------------------------------------------

def log_summary(
    normalized_df: pd.DataFrame,
    matched_df: pd.DataFrame,
    unmatched_bank_df: pd.DataFrame,
    unmatched_wallet_df: pd.DataFrame,
) -> None:
    """Log reconciliation statistics."""
    bank_count = int((normalized_df["source"] == "Bank").sum())
    wallet_count = int((normalized_df["source"] == "Wallet").sum())
    match_count = len(matched_df)

    logger.info("=" * 60)
    logger.info("RECONCILIATION SUMMARY")
    logger.info(
        "Total rows processed: %d (Bank: %d, Wallet: %d)",
        len(normalized_df),
        bank_count,
        wallet_count,
    )
    logger.info("Matched pairs: %d", match_count)
    logger.info("Unmatched bank records: %d", len(unmatched_bank_df))
    logger.info("Unmatched wallet records: %d", len(unmatched_wallet_df))

    if bank_count:
        logger.info("Bank match rate: %.1f%%", match_count / bank_count * 100)
    if wallet_count:
        logger.info("Wallet match rate: %.1f%%", match_count / wallet_count * 100)

    if not matched_df.empty:
        fee_matches = (matched_df["amount_difference"] > 0).sum()
        if fee_matches:
            logger.info(
                "Gateway-fee tolerant matches: %d (avg delta $%.2f)",
                fee_matches,
                matched_df.loc[
                    matched_df["amount_difference"] > 0, "amount_difference"
                ].mean(),
            )

    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_pipeline(config: dict[str, Any]) -> None:
    """Execute the full reconciliation pipeline end-to-end."""
    paths = config["file_paths"]
    thresholds = config["thresholds"]
    mappings = config["column_mappings"]
    generate_mock = config.get("pipeline", {}).get("generate_mock_data", True)

    if generate_mock:
        logger.info("Step 1: Generating synthetic CSV data")
        generate_synthetic_data(paths["bank_csv"], paths["wallet_csv"], config)
    else:
        logger.info("Step 1: Using existing CSV files")

    logger.info("Step 2: Loading and normalizing data")
    normalized_df = load_and_normalize(
        paths["bank_csv"],
        paths["wallet_csv"],
        mappings,
    )
    logger.info("Combined normalized rows: %d", len(normalized_df))

    logger.info("Step 3: Reconciling Bank vs Wallet")
    matched_df, unmatched_bank_df, unmatched_wallet_df = reconcile_transactions(
        normalized_df,
        thresholds,
    )

    logger.info("Step 4: Exporting to SQLite")
    export_to_sqlite(
        matched_df,
        unmatched_bank_df,
        unmatched_wallet_df,
        paths["sqlite_db"],
    )

    log_summary(normalized_df, matched_df, unmatched_bank_df, unmatched_wallet_df)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Multi-source financial reconciliation pipeline",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to config JSON (default: {DEFAULT_CONFIG_PATH})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    configure_logging()
    args = _parse_args()
    run_pipeline(load_config(args.config))
