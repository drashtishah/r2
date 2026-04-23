# methbooks/rules/event_handling/share_freeze.py
"""
Purpose: Implement share freeze commencing 5 business days before each Quarterly Index Review effective date; defer NOS/FIF adjustments from secondary offerings, private placements, and block sales occurring within the freeze period to the Index Review effective date.
Datapoints: security_id, nos, fif, dif, index_review_effective_date
Thresholds: FREEZE_PERIOD_BUSINESS_DAYS_BEFORE_REVIEW = 5
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "7 Share freeze" near line 5334.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

FREEZE_PERIOD_BUSINESS_DAYS_BEFORE_REVIEW = 5

_FREEZE_EXEMPT_EVENT_TYPES = {"ma", "split", "bonus_issue", "reverse_split", "rights_offering"}


def share_freeze(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "fif", "dif", "index_review_effective_date"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert FREEZE_PERIOD_BUSINESS_DAYS_BEFORE_REVIEW == 5, (
        f"share freeze commences 5 business days before Index Review effective date, "
        f"actual: {FREEZE_PERIOD_BUSINESS_DAYS_BEFORE_REVIEW}"
    )

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

    # Events during freeze period from secondary offerings, placements, block sales
    # must be deferred; M&A, splits, bonus issues, reverse splits, rights not subject to freeze.
    if "event_type" in out.columns and "is_in_freeze_period" in out.columns and "deferred_to_index_review" in out.columns:
        freeze_events = out.filter(
            pl.col("is_in_freeze_period")
            & ~pl.col("event_type").is_in(list(_FREEZE_EXEMPT_EVENT_TYPES))
        )
        not_deferred = freeze_events.filter(~pl.col("deferred_to_index_review").cast(pl.Boolean))
        assert not_deferred.is_empty(), (
            f"NOS/FIF adjustments during freeze period from non-exempt events must be deferred; "
            f"offending security_ids: {not_deferred['security_id'].to_list()}"
        )

    return out
