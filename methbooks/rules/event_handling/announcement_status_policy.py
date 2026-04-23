# methbooks/rules/event_handling/announcement_status_policy.py
"""
Purpose: Announce corporate event changes using four primary statuses: Acknowledged (within 5 business days of event announcement if impact > 1x Large Cap Cutoff), Undetermined, Expected (>= 10 business days advance), Confirmed (>= 2 business days advance).
Datapoints: security_id, float_adj_market_cap, country_index_large_cap_cutoff
Thresholds: ACKNOWLEDGED_THRESHOLD_LARGE_CAP_CUTOFF_MULTIPLE = 1, ACKNOWLEDGED_ADVANCE_BUSINESS_DAYS = 5, EXPECTED_ADVANCE_BUSINESS_DAYS = 10, CONFIRMED_ADVANCE_BUSINESS_DAYS = 2
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "8 General Announcement Policy for Corporate Events" near line 5355.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

ACKNOWLEDGED_THRESHOLD_LARGE_CAP_CUTOFF_MULTIPLE = 1
ACKNOWLEDGED_ADVANCE_BUSINESS_DAYS = 5
EXPECTED_ADVANCE_BUSINESS_DAYS = 10
CONFIRMED_ADVANCE_BUSINESS_DAYS = 2


def announcement_status_policy(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "float_adj_market_cap", "country_index_large_cap_cutoff"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert CONFIRMED_ADVANCE_BUSINESS_DAYS == 2, (
        f"confirmed status must be >= 2 business days advance, "
        f"actual: {CONFIRMED_ADVANCE_BUSINESS_DAYS}"
    )

    assert EXPECTED_ADVANCE_BUSINESS_DAYS >= CONFIRMED_ADVANCE_BUSINESS_DAYS, (
        f"expected status lead time must be >= confirmed lead time; "
        f"expected: {EXPECTED_ADVANCE_BUSINESS_DAYS}, confirmed: {CONFIRMED_ADVANCE_BUSINESS_DAYS}"
    )

    fam_min = float(out["float_adj_market_cap"].min())
    assert fam_min >= 0, f"float_adj_market_cap must be >= 0, actual min: {fam_min}"

    cutoff_min = float(out["country_index_large_cap_cutoff"].min())
    assert cutoff_min > 0, f"country_index_large_cap_cutoff must be > 0, actual min: {cutoff_min}"

    # IPO announcements: confirmed status only; no undetermined or expected.
    if "event_type" in out.columns and "announcement_status" in out.columns:
        ipo_rows = out.filter(pl.col("event_type") == "ipo")
        invalid_ipo_status = ipo_rows.filter(
            pl.col("announcement_status").is_in(["undetermined", "expected"])
        )
        assert invalid_ipo_status.is_empty(), (
            f"IPO announcements must use confirmed status only; "
            f"offending statuses: {invalid_ipo_status['announcement_status'].to_list()}"
        )

    return out
