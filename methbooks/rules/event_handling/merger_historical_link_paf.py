# methbooks/rules/event_handling/merger_historical_link_paf.py
"""
Purpose: Apply PAF on first trading day of merged entity to ensure price continuity with the continuing entity; PAF required only when exchange terms differ from 1-for-1.
Datapoints: security_id, closing_price, full_market_cap, gics_sector, country_of_classification
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.9 Historical Links and PAFs" near line 3872.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

_CONTINUATION_FACTORS = {"relative_market_cap", "industry_classification", "domicile", "legal_acquirer", "previous_size_segment"}


def merger_historical_link_paf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price", "full_market_cap", "gics_sector", "country_of_classification"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, f"closing_price must be > 0 on first trading day, actual min: {cp_min}"

    fmcap_min = float(out["full_market_cap"].min())
    assert fmcap_min >= 0, f"full_market_cap must be >= 0, actual min: {fmcap_min}"

    # PAF must be present when exchange ratio != 1-for-1.
    if "paf" in out.columns and "exchange_ratio" in out.columns:
        non_unity_ratio = out.filter(pl.col("exchange_ratio") != 1.0)
        missing_paf = non_unity_ratio.filter(pl.col("paf").is_null())
        assert missing_paf.is_empty(), (
            f"PAF must be applied to merged entity on first trading day when terms != 1-for-1; "
            f"missing PAF for security_ids: {missing_paf['security_id'].to_list()}"
        )

    return out
