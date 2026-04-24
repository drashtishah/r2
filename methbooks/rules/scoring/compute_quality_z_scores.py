"""
Purpose: Standardize each winsorized fundamental variable to zero mean and unit std
    within the Parent Index; negate D/E and Earnings Variability.
Datapoints: roe_winsorized, debt_to_equity_winsorized, earnings_variability_winsorized.
Thresholds: none.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "2.2.2 Calculating The Z-Scores" near line 161.
"""
from __future__ import annotations

import polars as pl


def compute_quality_z_scores(df: pl.DataFrame) -> pl.DataFrame:
    assert "roe_winsorized" in df.columns, f"roe_winsorized missing: {df.columns}"
    assert "debt_to_equity_winsorized" in df.columns, (
        f"debt_to_equity_winsorized missing: {df.columns}"
    )
    assert "earnings_variability_winsorized" in df.columns, (
        f"earnings_variability_winsorized missing: {df.columns}"
    )

    def _z(col: str, negate: bool) -> pl.Expr:
        non_null = df[col].drop_nulls()
        if non_null.is_empty():
            return pl.lit(None).cast(pl.Float64).alias(f"{col.replace('_winsorized', '')}_z_score")
        mean = float(non_null.mean())
        std = float(non_null.std())
        if std == 0 or std is None:
            return pl.lit(0.0).alias(f"{col.replace('_winsorized', '')}_z_score")
        sign = -1.0 if negate else 1.0
        out_col = col.replace("_winsorized", "_z_score")
        return (
            pl.when(pl.col(col).is_null())
            .then(pl.lit(None).cast(pl.Float64))
            .otherwise(sign * (pl.col(col) - mean) / std)
            .alias(out_col)
        )

    out = df.with_columns([
        _z("roe_winsorized", negate=False),
        _z("debt_to_equity_winsorized", negate=True),
        _z("earnings_variability_winsorized", negate=True),
    ])

    assert "roe_z_score" in out.columns, f"roe_z_score missing: {out.columns}"
    assert "debt_to_equity_z_score" in out.columns, f"debt_to_equity_z_score missing: {out.columns}"
    assert "earnings_variability_z_score" in out.columns, (
        f"earnings_variability_z_score missing: {out.columns}"
    )

    roe_mean = float(out["roe_z_score"].drop_nulls().mean() or 0.0)
    assert abs(roe_mean) < 1e-9, f"roe_z_score mean not approximately 0: {roe_mean}"

    # Negation check: de_z_score sum should have opposite sign to naive standardized de.
    de_non_null = df["debt_to_equity_winsorized"].drop_nulls()
    if not de_non_null.is_empty():
        de_mean = float(de_non_null.mean())
        de_std = float(de_non_null.std())
        if de_std and de_std > 0:
            naive_sum = float(((de_non_null - de_mean) / de_std).sum())
            negated_sum = float(out["debt_to_equity_z_score"].drop_nulls().sum())
            if abs(naive_sum) > 1e-9:
                assert (naive_sum > 0) != (negated_sum > 0), (
                    f"debt_to_equity_z_score not negated: naive_sum={naive_sum}, "
                    f"negated_sum={negated_sum}"
                )

    return out
