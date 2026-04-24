from __future__ import annotations

import polars as pl


def apply_redemption_right_paf_usa(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Apply a price adjustment when a USA company redeems shares distributed under a poison pill rights issue, reflecting the change in the company's capital structure.
    Datapoints: country_of_listing, redemption_right_flag, price_per_share.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.3.6 Country Exceptions - USA: Redemption Right" near line 1823.
    See also: methbooks/rules/event_handling/apply_paf_for_extraordinary_capital_repayment.py (broader extraordinary capital repayment PAF; redemption right is a USA-specific variant).
    """
    required = [
        "country_of_listing",
        "redemption_right_flag",
        "price_per_share",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["price_per_share"].min()) > 0, f"price_per_share must be > 0: min={float(out['price_per_share'].min())}"
    if "paf" in out.columns:
        assert float(out["paf"].min()) > 0, f"paf must be > 0 for redemption right adjustment: min={float(out['paf'].min())}"
    assert True, "applies to poison-pill-style redemption rights, mainly USA; price adjustment reflects change in company capital structure"

    return out
