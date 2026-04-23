# methbooks/rules/event_handling/tender_offer_friendly_deletion.py
"""
Purpose: Delete the target at the end of the initial offer period for friendly tender offers likely to succeed, or where the security's FIF is likely to decrease below 0.15.
Datapoints: security_id, fif, is_tender_friendly, tender_offer_likely_success
Thresholds: FIF_DELETION_THRESHOLD = 0.15
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.2.3 Implementation timing" near line 583.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

FIF_DELETION_THRESHOLD = 0.15


def tender_offer_friendly_deletion(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "fif", "is_tender_friendly", "tender_offer_likely_success"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    fif_min = float(out["fif"].min())
    assert fif_min >= 0, (
        f"fif must be >= 0 for all securities, actual min fif: {fif_min}"
    )

    fif_max = float(out["fif"].max())
    assert fif_max <= 1, (
        f"fif must be <= 1 for all securities, actual max fif: {fif_max}"
    )

    # Security must not be reinstated if tender offer subsequently declared unsuccessful.
    if "is_reinstated" in out.columns and "tender_offer_failed_after_deletion" in out.columns:
        invalid = out.filter(
            pl.col("is_reinstated") & pl.col("tender_offer_failed_after_deletion")
        )
        assert invalid.is_empty(), (
            f"security must not be reinstated after tender offer declared unsuccessful; "
            f"offending security_ids: {invalid['security_id'].to_list()}"
        )

    return out
