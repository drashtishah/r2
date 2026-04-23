# methbooks/rules/event_handling/country_classification_review_at_event.py
"""
Purpose: Review index constituent country of classification when a change of listing, change of incorporation, or delisting occurs; change COC at event implementation if constituent becomes ineligible or if geographical profile fundamentally changes; otherwise defer COC review to next Index Review.
Datapoints: security_id, country_of_classification, primary_exchange
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "5.5 Country Classification Review" near line 5197.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def country_classification_review_at_event(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "country_of_classification", "primary_exchange"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert "country_of_classification" in out.columns, (
        "country_of_classification must be present; reviewed for all newly listed securities "
        "(spun-offs, IPOs, mergers)"
    )

    assert "primary_exchange" in out.columns, (
        "primary_exchange must be present to determine listing-driven COC review trigger"
    )

    # COC changes unrelated to corporate events must not be implemented at event time.
    if "coc_change_reason" in out.columns and "is_event_driven_coc_change" in out.columns:
        non_event_coc = out.filter(
            ~pl.col("is_event_driven_coc_change")
            & pl.col("coc_change_reason").is_not_null()
        )
        assert non_event_coc.is_empty(), (
            f"COC changes unrelated to corporate events must be implemented at relevant "
            f"Index Reviews per GIMI Appendix III, not at event time; "
            f"offending security_ids: {non_event_coc['security_id'].to_list()}"
        )

    return out
