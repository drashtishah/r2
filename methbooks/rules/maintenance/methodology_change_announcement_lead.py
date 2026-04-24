"""
Purpose: Ensure methodology changes are announced at least 1 month before implementation, unless the EIC determines a faster timeline is required.
Datapoints: change_announcement_date, change_effective_date, eic_fast_track_flag.
Thresholds: STANDARD_LEAD_DAYS = 30.
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Methodology and Index Consultation Process" near line 372.
See also: methbooks/rules/maintenance/index_termination_notice_standard.py (notice requirement pattern).
"""
from __future__ import annotations
import polars as pl

STANDARD_LEAD_DAYS = 30


def methodology_change_announcement_lead(df: pl.DataFrame) -> pl.DataFrame:
    required = ["change_announcement_date", "change_effective_date", "eic_fast_track_flag"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    standard = out.filter(~pl.col("eic_fast_track_flag"))
    if standard.height > 0:
        lead_days = (
            pl.col("change_effective_date") - pl.col("change_announcement_date")
        ).dt.total_days()
        short_lead = standard.filter(lead_days < STANDARD_LEAD_DAYS)
        assert short_lead.height == 0, (
            f"methodology changes without EIC fast-track must be announced at least "
            f"{STANDARD_LEAD_DAYS} days before implementation; short_lead_count={short_lead.height}"
        )
    return out
