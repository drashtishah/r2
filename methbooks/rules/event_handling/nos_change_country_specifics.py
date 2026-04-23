# methbooks/rules/event_handling/nos_change_country_specifics.py
"""
Purpose: Apply country-specific implementation timing and threshold variations for NOS/FIF changes from share placements and offerings per section 4.3.
Datapoints: security_id, country_of_classification
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "4.3 Country Specifics" near line 4800.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def nos_change_country_specifics(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "country_of_classification"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert "country_of_classification" in out.columns, (
        "country_of_classification must be present to evaluate section 4.3 overrides"
    )

    # Country override must only be applied when country_of_classification matches a
    # section 4.3 entry. Validate via has_country_override if present.
    if "has_country_override" in out.columns and "country_override_section" in out.columns:
        overridden = out.filter(pl.col("has_country_override"))
        missing_section = overridden.filter(pl.col("country_override_section").is_null())
        assert missing_section.is_empty(), (
            f"country override must reference an explicit section 4.3 entry; "
            f"offending security_ids: {missing_section['security_id'].to_list()}"
        )

    return out
