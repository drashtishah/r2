"""
Purpose: Discontinue an index when eligible security count falls below the methodology-defined minimum, since the index can no longer be calculated in accordance with the applicable methodology.
Datapoints: eligible_security_count, methodology_minimum_securities, discontinued_flag.
Thresholds: none (minimum varies per methodology; see clarifications_needed).
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Index Termination Policy" near line 463.
See also: methbooks/rules/maintenance/index_termination_notice_standard.py (termination notice rule).
"""
from __future__ import annotations
import polars as pl


def index_discontinued_minimum_securities(df: pl.DataFrame) -> pl.DataFrame:
    required = ["eligible_security_count", "methodology_minimum_securities", "discontinued_flag"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    below_min = out.filter(
        pl.col("eligible_security_count") < pl.col("methodology_minimum_securities")
    )
    if below_min.height > 0:
        not_discontinued = below_min.filter(~pl.col("discontinued_flag"))
        assert not_discontinued.height == 0, (
            f"discontinued_flag must be True when eligible_security_count < methodology_minimum_securities; "
            f"not_discontinued_count={not_discontinued.height}"
        )
    return out
