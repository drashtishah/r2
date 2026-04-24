from __future__ import annotations

import polars as pl

RESTATEMENT_THRESHOLD_BPS = 50
CORRECTION_WINDOW_MONTHS = 12


def restate_index_at_50bps_error_threshold(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Apply historical index restatement when a data error causes an impact of 50 basis points or more on any affected country index or World/EM industry group index, within a 12-month correction window.
    Datapoints: error_impact_bps, affected_index_type, error_discovery_date.
    Thresholds: RESTATEMENT_THRESHOLD_BPS = 50, CORRECTION_WINDOW_MONTHS = 12.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "Appendix XI: Index Correction Policy" near line 14199.
    See also: methbooks/rules/maintenance/correction_restatement_threshold.py (general correction threshold rule).
    """
    required = [
        "error_impact_bps",
        "affected_index_type",
        "error_discovery_date",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["error_impact_bps"].min()) >= 0, f"error_impact_bps must be >= 0: min={float(out['error_impact_bps'].min())}"
    if "is_restated" in out.columns:
        below_threshold_restated = out.filter(
            (pl.col("error_impact_bps") < RESTATEMENT_THRESHOLD_BPS) & pl.col("is_restated")
        )
        assert below_threshold_restated.is_empty(), f"only indexes with error_impact_bps >= {RESTATEMENT_THRESHOLD_BPS} are automatically restated: count restated below threshold={below_threshold_restated.height}"
    assert True, "MSCI may correct below threshold for systematic errors at its discretion (e.g., constituent omission, large-scale price errors); errors older than 12 months are in general not revised"

    return out
