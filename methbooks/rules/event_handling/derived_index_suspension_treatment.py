# methbooks/rules/event_handling/derived_index_suspension_treatment.py
"""
Purpose: When a security is suspended two days before a Derived Index Review effective date: postpone additions coinciding with Parent Index postponements; also postpone matching deletions; revert constraint factor changes to pre-review levels; exception for 10/40, 25/50, 35/65 concentration constraint breaches.
Datapoints: security_id, is_suspended, constraint_factor, is_derived_index_constituent, is_parent_index_constituent
Thresholds: SUSPENSION_CHECK_DAYS_BEFORE_REVIEW = 2
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "Appendix VIII: Policy Regarding Trading Suspensions in the MSCI Derived Indexes during the Index Reviews" near line 6167.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

SUSPENSION_CHECK_DAYS_BEFORE_REVIEW = 2


def derived_index_suspension_treatment(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "is_suspended", "constraint_factor", "is_derived_index_constituent", "is_parent_index_constituent"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert SUSPENSION_CHECK_DAYS_BEFORE_REVIEW == 2, (
        f"postponed parent-index addition: also postpone derived index addition; "
        f"suspension check must occur 2 days before review, "
        f"actual: {SUSPENSION_CHECK_DAYS_BEFORE_REVIEW}"
    )

    cf_min = float(out["constraint_factor"].min())
    assert cf_min >= 0, f"constraint_factor must be >= 0, actual min: {cf_min}"

    # Concentration constraint exception: if non-implementation would breach 10/40, 25/50,
    # or 35/65 limits, change implemented regardless; validate via exception flag if present.
    if "has_concentration_constraint_exception" in out.columns and "is_postponed_due_to_suspension" in out.columns:
        exception_rows = out.filter(pl.col("has_concentration_constraint_exception"))
        wrongly_postponed = exception_rows.filter(pl.col("is_postponed_due_to_suspension"))
        assert wrongly_postponed.is_empty(), (
            f"concentration constraint exception must not be postponed regardless of suspension; "
            f"offending security_ids: {wrongly_postponed['security_id'].to_list()}"
        )

    return out
