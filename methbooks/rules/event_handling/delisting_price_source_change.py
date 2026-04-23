# methbooks/rules/event_handling/delisting_price_source_change.py
"""
Purpose: When a security delists from its primary exchange but maintains another listing, switch price source to the other exchange and maintain security in MSCI Indexes if the new listing does not make the company ineligible.
Datapoints: security_id, primary_exchange, is_eligible_on_new_exchange, country_of_classification
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "5.4 Delistings from Primary Exchange" near line 5186.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def delisting_price_source_change(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "primary_exchange", "is_eligible_on_new_exchange", "country_of_classification"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert "country_of_classification" in out.columns, (
        "country of classification must be present; reviewed at Index Review following listing change"
    )

    # Securities ineligible on new exchange must not be maintained.
    if "is_index_constituent" in out.columns and "has_delisted_primary" in out.columns:
        delisted = out.filter(pl.col("has_delisted_primary"))
        ineligible_maintained = delisted.filter(
            ~pl.col("is_eligible_on_new_exchange") & pl.col("is_index_constituent")
        )
        assert ineligible_maintained.is_empty(), (
            f"securities ineligible on new exchange must not remain index constituents; "
            f"offending security_ids: {ineligible_maintained['security_id'].to_list()}"
        )

    return out
