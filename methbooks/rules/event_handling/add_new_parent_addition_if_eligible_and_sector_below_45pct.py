"""
Purpose: Add a security newly included in the Parent Index to the Selection Index on the date of its Parent Index inclusion only if it meets Section 2.2 eligibility criteria and cumulative ff-mcap coverage of its GICS sector is currently below 45%.
Datapoints: is_eligible_section_2_2, sector_current_ff_mcap_coverage, gics_sector.
Thresholds: SECTOR_COVERAGE_ADDITION_THRESHOLD = 0.45 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "3.3 Ongoing Event-Related Maintenance" near line 460.
See also: methbooks/rules/event_handling/add_quarterly_if_eligible_and_sector_below_45pct.py (same coverage gate applied at Quarterly Index Review).
"""
from __future__ import annotations
import polars as pl

SECTOR_COVERAGE_ADDITION_THRESHOLD = 0.45

def add_new_parent_addition_if_eligible_and_sector_below_45pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        pl.col("is_eligible_section_2_2") & (pl.col("sector_current_ff_mcap_coverage") < SECTOR_COVERAGE_ADDITION_THRESHOLD)
    )
    assert "is_eligible_section_2_2" in out.columns, f"is_eligible_section_2_2 column missing: {out.columns}"
    assert "sector_current_ff_mcap_coverage" in out.columns, f"sector_current_ff_mcap_coverage column missing: {out.columns}"
    disallowed_by_coverage = out.filter(pl.col("sector_current_ff_mcap_coverage") >= SECTOR_COVERAGE_ADDITION_THRESHOLD)
    assert disallowed_by_coverage.height == 0, (
        f"securities added to sector with coverage >= {SECTOR_COVERAGE_ADDITION_THRESHOLD}: "
        f"{disallowed_by_coverage.height} rows, coverages={disallowed_by_coverage['sector_current_ff_mcap_coverage'].to_list()[:5]}"
    )
    ineligible_added = out.filter(~pl.col("is_eligible_section_2_2"))
    assert ineligible_added.height == 0, (
        f"ineligible securities added: {ineligible_added.height} rows"
    )
    return out
