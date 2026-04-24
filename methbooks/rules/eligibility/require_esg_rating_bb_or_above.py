"""
Purpose: Restrict the index to companies with a minimum MSCI ESG Rating of BB, ensuring only companies with demonstrated ability to manage ESG risks are eligible.
Datapoints: msci_esg_rating.
Thresholds: MIN_ESG_RATING = 'BB' (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "2.2.1 ESG Ratings Eligibility" near line 141.
See also: methbooks/rules/eligibility/require_controversies_score_3_or_above.py (companion eligibility screen applied after this rule).
"""
from __future__ import annotations
import polars as pl

MIN_ESG_RATING = "BB"
_ELIGIBLE_RATINGS = {"AAA", "AA", "A", "BBB", "BB"}

def require_esg_rating_bb_or_above(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("msci_esg_rating").is_in(list(_ELIGIBLE_RATINGS)))
    assert "msci_esg_rating" in out.columns, f"msci_esg_rating column missing after filter: {out.columns}"
    assert out["msci_esg_rating"].null_count() == 0, f"null msci_esg_rating values after filter: {out['msci_esg_rating'].null_count()}"
    ineligible = out.filter(~pl.col("msci_esg_rating").is_in(list(_ELIGIBLE_RATINGS)))
    assert ineligible.height == 0, f"rows with msci_esg_rating below BB survived: {ineligible['msci_esg_rating'].to_list()}"
    return out
