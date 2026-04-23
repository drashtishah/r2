# methbooks/rules/event_handling/large_secondary_offering_lead_time.py
"""
Purpose: Require at least 10 business days lead time to evaluate large secondary offerings for early inclusion.
Datapoints: security_id
Thresholds: LARGE_SECONDARY_OFFERING_LEAD_TIME_BUSINESS_DAYS = 10
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "6 IPOs and Other Early Inclusions" near line 5229.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

LARGE_SECONDARY_OFFERING_LEAD_TIME_BUSINESS_DAYS = 10


def large_secondary_offering_lead_time(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert LARGE_SECONDARY_OFFERING_LEAD_TIME_BUSINESS_DAYS == 10, (
        f"lead time must be >= 10 business days before assessment, "
        f"actual: {LARGE_SECONDARY_OFFERING_LEAD_TIME_BUSINESS_DAYS}"
    )

    if "lead_time_business_days" in out.columns:
        insufficient = out.filter(
            pl.col("lead_time_business_days") < LARGE_SECONDARY_OFFERING_LEAD_TIME_BUSINESS_DAYS
        )
        assert insufficient.is_empty(), (
            f"lead time must be >= {LARGE_SECONDARY_OFFERING_LEAD_TIME_BUSINESS_DAYS} business days; "
            f"offending security_ids with lead times: "
            f"{insufficient[['security_id', 'lead_time_business_days']].to_dicts()}"
        )

    return out
