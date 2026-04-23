# methbooks/rules/event_handling/ma_country_specific_us_canada_uk_overrides.py
"""
Purpose: Apply country-specific implementation variations for M&A in US, Canada, and UK per sections 2.1.4.1/2.1.4.2/2.1.4.3.
Datapoints: security_id, country_of_classification
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.1.4 Country Specifics" near line 480.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

_OVERRIDE_COUNTRIES = {"US", "CA", "GB"}


def ma_country_specific_us_canada_uk_overrides(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "country_of_classification"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    if "has_country_override" in out.columns:
        invalid = out.filter(
            pl.col("has_country_override")
            & ~pl.col("country_of_classification").is_in(list(_OVERRIDE_COUNTRIES))
        )
        assert invalid.is_empty(), (
            f"country override applied only when country_of_classification in "
            f"{_OVERRIDE_COUNTRIES}, offending rows: "
            f"{invalid[['security_id', 'country_of_classification']].to_dicts()}"
        )

    return out
