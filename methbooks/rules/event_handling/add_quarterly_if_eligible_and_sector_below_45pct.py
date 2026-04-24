"""
Purpose: Make additions at each Quarterly Index Review only to sectors where cumulative ff-mcap coverage has fallen below 45% (10% buffer on the 50% target), selecting from Section 2.2 eligible securities until 50% coverage is restored.
Datapoints: is_eligible_section_2_2, sector_current_ff_mcap_coverage, gics_sector.
Thresholds: SECTOR_COVERAGE_ADDITION_THRESHOLD = 0.45, TARGET_COVERAGE = 0.5, BUFFER_PCT = 0.1.
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "3.2 Quarterly Index Reviews" near line 421.
See also: methbooks/rules/event_handling/add_new_parent_addition_if_eligible_and_sector_below_45pct.py (same coverage gate applied at event-driven intra-review additions).
"""
from __future__ import annotations
import polars as pl

SECTOR_COVERAGE_ADDITION_THRESHOLD = 0.45
TARGET_COVERAGE = 0.5
BUFFER_PCT = 0.1

def add_quarterly_if_eligible_and_sector_below_45pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        pl.col("is_eligible_section_2_2") & (pl.col("sector_current_ff_mcap_coverage") < SECTOR_COVERAGE_ADDITION_THRESHOLD)
    )
    assert "sector_current_ff_mcap_coverage" in out.columns, f"sector_current_ff_mcap_coverage column missing: {out.columns}"
    disallowed = out.filter(pl.col("sector_current_ff_mcap_coverage") >= SECTOR_COVERAGE_ADDITION_THRESHOLD)
    assert disallowed.height == 0, (
        f"additions made to sectors with coverage >= {SECTOR_COVERAGE_ADDITION_THRESHOLD}: "
        f"{disallowed.height} rows, coverages={disallowed['sector_current_ff_mcap_coverage'].to_list()[:5]}"
    )
    ineligible = out.filter(~pl.col("is_eligible_section_2_2"))
    assert ineligible.height == 0, (
        f"ineligible securities added at quarterly review: {ineligible.height} rows"
    )
    return out
