"""MSCI Corporate Events methodology module."""
from __future__ import annotations

import math
import random
from pathlib import Path

import numpy as np
import polars as pl

from methbooks.mock_universe import build_base_universe

ROWS = 2000
_CSV = Path(__file__).parent / "corporate_events_data_dictionary.csv"


def build_mock_data() -> pl.DataFrame:
    """Return a 2000-row DataFrame with all corporate_events datapoints."""
    base = build_base_universe()
    rng = np.random.default_rng(42)
    py_rng = random.Random(42)

    extra_cols: dict[str, list] = {
        "fif": rng.uniform(0.05, 1.0, ROWS).tolist(),
        "dif": rng.uniform(0.05, 1.0, ROWS).tolist(),
        "nos": [
            int(v)
            for v in np.exp(
                rng.uniform(math.log(100_000), math.log(10_000_000_000), ROWS)
            ).tolist()
        ],
        "closing_price": rng.lognormal(4.0, 1.5, ROWS).tolist(),
        "full_market_cap": np.exp(
            rng.uniform(math.log(1e8), math.log(1e13), ROWS)
        ).tolist(),
        "float_adj_market_cap": np.exp(
            rng.uniform(math.log(5e7), math.log(5e12), ROWS)
        ).tolist(),
        "size_segment": [
            py_rng.choice(["Standard", "SmallCap", "MicroCap"]) for _ in range(ROWS)
        ],
        "constraint_factor": rng.uniform(0.0, 1.5, ROWS).tolist(),
        "variable_weighting_factor": rng.uniform(0.5, 1.5, ROWS).tolist(),
        "country_of_classification": [
            py_rng.choice(["US", "GB", "DE", "JP", "CN", "BR", "CA", "AU", "IN", "CH", "other"])
            for _ in range(ROWS)
        ],
        "is_suspended": [py_rng.random() < 0.02 for _ in range(ROWS)],
        "is_technical_suspension": [py_rng.random() < 0.5 for _ in range(ROWS)],
        "consecutive_suspension_days": [
            py_rng.choice([0, 10, 25, 50, 75, 100, 120]) for _ in range(ROWS)
        ],
        "is_index_constituent": [py_rng.random() < 0.85 for _ in range(ROWS)],
        "is_parent_index_constituent": [py_rng.random() < 0.9 for _ in range(ROWS)],
        "is_derived_index_constituent": [py_rng.random() < 0.6 for _ in range(ROWS)],
        "interim_size_segment_cutoff": np.exp(
            rng.uniform(math.log(1e8), math.log(5e9), ROWS)
        ).tolist(),
        "is_stock_connect_ashare": [py_rng.random() < 0.05 for _ in range(ROWS)],
        "is_stock_connect_trading_day": [py_rng.random() < 0.9 for _ in range(ROWS)],
        "is_market_neutral_event": [py_rng.random() < 0.4 for _ in range(ROWS)],
        "trading_calendar_type": [
            py_rng.choice(["mon_fri", "sun_mon_fri_intermediary", "sun_thu"])
            for _ in range(ROWS)
        ],
        "is_trading": [py_rng.random() < 0.98 for _ in range(ROWS)],
        "is_tender_friendly": [py_rng.random() < 0.7 for _ in range(ROWS)],
        "tender_offer_likely_success": [py_rng.random() < 0.6 for _ in range(ROWS)],
        "offer_type": [
            py_rng.choice(["fixed_price", "dutch_auction", "split_off", "exchange_offer"])
            for _ in range(ROWS)
        ],
        "conversion_type": [
            py_rng.choice(["mandatory_full", "mandatory_partial", "voluntary", "periodical"])
            for _ in range(ROWS)
        ],
        "share_class": [
            py_rng.choice(["A", "B", "H", "common", "preferred"]) for _ in range(ROWS)
        ],
        "consideration_type": [
            py_rng.choice(["cash", "stock_for_stock", "mixed"]) for _ in range(ROWS)
        ],
        "is_extraordinary_distribution": [py_rng.random() < 0.1 for _ in range(ROWS)],
        "special_dividend_amount": rng.lognormal(0.0, 1.0, ROWS).tolist(),
        "default_distribution_type": [
            py_rng.choice(["cash", "stock", "unspecified"]) for _ in range(ROWS)
        ],
        "subscription_price": rng.lognormal(3.5, 1.5, ROWS).tolist(),
        "shares_before": [
            int(v)
            for v in np.exp(
                rng.uniform(math.log(100_000), math.log(1e10), ROWS)
            ).tolist()
        ],
        "shares_issued": [
            int(v)
            for v in np.exp(
                rng.uniform(math.log(1_000), math.log(1e9), ROWS)
            ).tolist()
        ],
        "shares_acquired": [
            int(v)
            for v in np.exp(
                rng.uniform(math.log(1_000), math.log(1e9), ROWS)
            ).tolist()
        ],
        "rights_price": rng.lognormal(0.0, 1.0, ROWS).tolist(),
        "forthcoming_gross_dividend": rng.lognormal(0.0, 0.5, ROWS).tolist(),
        "offer_price": rng.lognormal(3.5, 1.5, ROWS).tolist(),
        "is_fully_underwritten": [py_rng.random() < 0.3 for _ in range(ROWS)],
        "is_pending_index_review_action": [py_rng.random() < 0.1 for _ in range(ROWS)],
        "placement_investor_type": [
            py_rng.choice(["institutional", "strategic", "unknown"]) for _ in range(ROWS)
        ],
        "is_price_limit_active": [py_rng.random() < 0.05 for _ in range(ROWS)],
        "exchange_closing_time": [
            py_rng.choice(["13:00", "14:00", "16:00", "17:00"]) for _ in range(ROWS)
        ],
        "is_otc_price_available": [py_rng.random() < 0.3 for _ in range(ROWS)],
        "primary_exchange": [
            py_rng.choice(["NYSE", "NASDAQ", "LSE", "TSE", "HKEX", "SSE", "other"])
            for _ in range(ROWS)
        ],
        "is_eligible_on_new_exchange": [py_rng.random() < 0.8 for _ in range(ROWS)],
        "gics_sector": [
            py_rng.choice([
                "Energy", "Materials", "Industrials", "Consumer Discretionary",
                "Consumer Staples", "Health Care", "Financials", "Information Technology",
                "Communication Services", "Utilities", "Real Estate",
            ])
            for _ in range(ROWS)
        ],
        "event_type": [
            py_rng.choice([
                "primary_offering", "secondary_offering", "block_sale",
                "debt_equity_swap", "convertible_conversion",
            ])
            for _ in range(ROWS)
        ],
        "index_review_effective_date": [
            py_rng.choice(["2026-02-27", "2026-05-29", "2026-08-31", "2026-11-30"])
            for _ in range(ROWS)
        ],
        "country_index_large_cap_cutoff": np.exp(
            rng.uniform(math.log(1e9), math.log(1e11), ROWS)
        ).tolist(),
    }

    return pl.concat([base, pl.DataFrame(extra_cols)], how="horizontal")


def get_data_dictionary() -> pl.DataFrame:
    """Return the corporate_events data dictionary as a polars DataFrame."""
    return pl.read_csv(_CSV)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    """Validate df against corporate_events invariants and return it unchanged."""
    required_base = ["security_id", "weight"]
    missing = [c for c in required_base if c not in df.columns]
    assert not missing, f"missing base columns: {missing}"

    if "fif" in df.columns:
        fif_min = float(df["fif"].min())
        fif_max = float(df["fif"].max())
        assert 0.0 <= fif_min and fif_max <= 1.0, f"fif out of [0,1]: min={fif_min}, max={fif_max}"

    if "dif" in df.columns:
        dif_min = float(df["dif"].min())
        dif_max = float(df["dif"].max())
        assert 0.0 <= dif_min and dif_max <= 1.0, f"dif out of [0,1]: min={dif_min}, max={dif_max}"

    if "nos" in df.columns:
        nos_min = float(df["nos"].min())
        assert nos_min >= 0, f"nos negative: min={nos_min}"

    if "constraint_factor" in df.columns:
        cf_min = float(df["constraint_factor"].min())
        assert cf_min >= 0, f"constraint_factor negative: min={cf_min}"

    if "variable_weighting_factor" in df.columns:
        vwf_min = float(df["variable_weighting_factor"].min())
        assert vwf_min >= 0, f"variable_weighting_factor negative: min={vwf_min}"

    weight_sum = float(df["weight"].sum())
    assert abs(weight_sum - 1.0) < 1e-6, f"weights do not sum to 1: sum={weight_sum}"

    weight_min = float(df["weight"].min())
    assert weight_min >= 0, f"negative weight found: min={weight_min}"

    return df
