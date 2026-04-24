"""
Purpose: Delete existing constituent acquired by non-Index company; block non-constituent
    acquirer from being added.
Datapoints: security_id, is_current_constituent, acquirer_is_index_constituent.
Thresholds: none.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "3.2 Ongoing Event Related Changes" near line 353.
"""
from __future__ import annotations

import polars as pl


def quality_ma_nonconstituent_acquirer_treatment(df: pl.DataFrame) -> pl.DataFrame:
    assert "security_id" in df.columns, f"security_id missing: {df.columns}"
    assert "is_current_constituent" in df.columns, (
        f"is_current_constituent missing: {df.columns}"
    )
    assert "acquirer_is_index_constituent" in df.columns, (
        f"acquirer_is_index_constituent missing: {df.columns}"
    )

    # Remove acquired constituents (constituent acquired by non-index company).
    # Also remove non-constituent acquirers (acquirer_is_index_constituent=False, not constituent).
    out = df.filter(
        ~(pl.col("acquirer_is_index_constituent") == False)  # noqa: E712
    )

    out_ids = set(out["security_id"].to_list())

    # Acquired constituents must be absent.
    acq_constituents = df.filter(
        (pl.col("is_current_constituent") == True)  # noqa: E712
        & (pl.col("acquirer_is_index_constituent") == False)  # noqa: E712
    )
    still_present = [sid for sid in acq_constituents["security_id"].to_list() if sid in out_ids]
    assert len(still_present) == 0, (
        f"acquired constituents still in output: {still_present}"
    )

    # Non-constituent acquirers must be absent.
    nonconstit_acq = df.filter(
        (pl.col("is_current_constituent") == False)  # noqa: E712
        & (pl.col("acquirer_is_index_constituent") == False)  # noqa: E712
    )
    still_present2 = [sid for sid in nonconstit_acq["security_id"].to_list() if sid in out_ids]
    assert len(still_present2) == 0, (
        f"non-constituent acquirers still in output: {still_present2}"
    )

    return out
