# methbooks/rules/event_handling/reverse_spinoff_history_link.py
"""
Purpose: When spun-off entity is determined to be the continuation of the former parent, apply PAF to the spun-off entity on ex-date and reassign parent identifiers to the spun-off.
Datapoints: security_id, full_market_cap, gics_sector, country_of_classification, size_segment
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.8.3 Reverse spinoffs" near line 3183.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def reverse_spinoff_history_link(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "full_market_cap", "gics_sector", "country_of_classification", "size_segment"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    fmcap_min = float(out["full_market_cap"].min())
    assert fmcap_min >= 0, f"full_market_cap must be >= 0, actual min: {fmcap_min}"

    # When is_reverse_spinoff is set, the spun-off entity must inherit parent identifiers.
    if "is_reverse_spinoff" in out.columns and "inherited_parent_id" in out.columns:
        reverse_rows = out.filter(pl.col("is_reverse_spinoff"))
        missing_id = reverse_rows.filter(pl.col("inherited_parent_id").is_null())
        assert missing_id.is_empty(), (
            f"market identifiers of spun-off entity must be assigned to former parent on ex-date; "
            f"offending security_ids: {missing_id['security_id'].to_list()}"
        )

    if "paf" in out.columns and "is_reverse_spinoff" in out.columns:
        reverse_rows = out.filter(pl.col("is_reverse_spinoff"))
        missing_paf = reverse_rows.filter(pl.col("paf").is_null())
        assert missing_paf.is_empty(), (
            f"PAF must be applied to spun-off entity in reverse spinoff; "
            f"missing PAF for: {missing_paf['security_id'].to_list()}"
        )

    return out
