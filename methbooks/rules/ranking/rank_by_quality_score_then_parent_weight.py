"""
Purpose: Assign descending rank by Quality Score; break ties using Parent Index weight.
Datapoints: quality_score, parent_index_weight.
Thresholds: none.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "2.3 Security Selection" near line 213.
"""
from __future__ import annotations

import polars as pl


def rank_by_quality_score_then_parent_weight(df: pl.DataFrame) -> pl.DataFrame:
    assert "quality_score" in df.columns, f"quality_score missing: {df.columns}"
    assert "parent_index_weight" in df.columns, f"parent_index_weight missing: {df.columns}"

    out = (
        df.sort(["quality_score", "parent_index_weight"], descending=[True, True])
        .with_row_index(name="quality_rank", offset=1)
    )

    assert "quality_rank" in out.columns, f"quality_rank missing: {out.columns}"

    ranks = out["quality_rank"].to_list()
    expected = list(range(1, out.height + 1))
    assert ranks == expected, (
        f"quality_rank not unique integers 1..{out.height}: first mismatch at "
        f"index {next(i for i, (a, b) in enumerate(zip(ranks, expected)) if a != b)}"
    )

    # Check tie-breaking: for rows with equal quality_score, parent_index_weight is descending.
    qs = out["quality_score"].to_list()
    pw = out["parent_index_weight"].to_list()
    for i in range(1, len(qs)):
        if qs[i] == qs[i - 1]:
            assert pw[i] <= pw[i - 1] + 1e-12, (
                f"tie-breaking violated at rank {i+1}: quality_score={qs[i]}, "
                f"parent_index_weight={pw[i]} > {pw[i-1]}"
            )

    return out
