"""
Purpose: Restate history when an error affects a large number of securities (e.g., a third of the prices in a particular market) even when aggregate performance impact is not deemed important.
Datapoints: affected_security_count, total_market_security_count, restatement_flag.
Thresholds: none (the large-scale threshold is not fixed in this document; see clarifications_needed).
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Corrections Policy" near line 489.
See also: methbooks/rules/maintenance/constituent_omission_restatement_override.py (omission override rule).
"""
from __future__ import annotations
import polars as pl


def large_scale_error_restatement_override(df: pl.DataFrame) -> pl.DataFrame:
    required = ["affected_security_count", "total_market_security_count", "restatement_flag"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    # Technical invariant: counts must be non-negative.
    asc_min = int(out["affected_security_count"].min())
    assert asc_min >= 0, (
        f"affected_security_count must be non-negative; min={asc_min}"
    )
    tsc_min = int(out["total_market_security_count"].min())
    assert tsc_min >= 0, (
        f"total_market_security_count must be non-negative; min={tsc_min}"
    )
    # affected cannot exceed total
    invalid = out.filter(
        pl.col("affected_security_count") > pl.col("total_market_security_count")
    )
    assert invalid.height == 0, (
        f"affected_security_count must not exceed total_market_security_count; "
        f"invalid_count={invalid.height}"
    )
    return out
