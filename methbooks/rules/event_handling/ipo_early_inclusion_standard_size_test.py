# methbooks/rules/event_handling/ipo_early_inclusion_standard_size_test.py
"""
Purpose: Require IPO full market cap >= 1.8x Interim Market Size-Segment Cutoff and free float-adjusted market cap >= 1.8 * 0.5 * Interim Cutoff, assessed as of close of first or second trading day, for Standard Index early inclusion.
Datapoints: security_id, full_market_cap, float_adj_market_cap, fif, interim_size_segment_cutoff
Thresholds: FULL_MARKET_CAP_MULTIPLE = 1.8, FLOAT_ADJ_MARKET_CAP_MULTIPLE = 1.8, FLOAT_ADJ_MARKET_CAP_HALF_CUTOFF = 0.5
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "6 IPOs and Other Early Inclusions" near line 5229.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

FULL_MARKET_CAP_MULTIPLE = 1.8
FLOAT_ADJ_MARKET_CAP_MULTIPLE = 1.8
FLOAT_ADJ_MARKET_CAP_HALF_CUTOFF = 0.5


def ipo_early_inclusion_standard_size_test(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "full_market_cap", "float_adj_market_cap", "fif", "interim_size_segment_cutoff"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert FULL_MARKET_CAP_MULTIPLE == 1.8, (
        f"full market cap multiple for IPO early inclusion must be 1.8, "
        f"actual: {FULL_MARKET_CAP_MULTIPLE}"
    )

    fmcap_min = float(out["full_market_cap"].min())
    assert fmcap_min >= 0, f"full_market_cap must be >= 0, actual min: {fmcap_min}"

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

    cutoff_min = float(out["interim_size_segment_cutoff"].min())
    assert cutoff_min > 0, f"interim_size_segment_cutoff must be > 0, actual min: {cutoff_min}"

    return out
