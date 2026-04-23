# methbooks conventions

Function contracts, assertion style, and the data-dictionary contract that
every rule and methodology in this tree obeys. Read this before writing or
reviewing any rule or methodology module.

## 1. Rule function shape

Default signature is `def <name>(df: pl.DataFrame) -> pl.DataFrame`. Pure, no
side effects.

Minimise kwargs. Add a keyword argument only when the same rule is used at
two or more different thresholds across methodologies; otherwise bake the
threshold as a module-level constant in the rule (or in the methodology
file when the rule itself is generic).

Eligibility rules filter rows via `df.filter(...)`. Weighting rules modify
the `weight` column.

End every rule with asserts covering both technical invariants (required
columns present, weight sum, non-negative weights) and the business or
methodology logic the rule claims (e.g. no security above
`max_stock_weight`, no sector above cap). Every assert message is an
f-string that prints the offending value, so a failure reads like a bug
report.

## 2. Docstring template

```
Purpose: one sentence on why this rule exists (business reason).
Datapoints: columns read.
Thresholds: methbook-specific constants (or kwargs if reused across meths).
Source: methbooks/data/markdown/<file>.md section "<heading>" near line <n>.
See also: methbooks/rules/<category>/<name>.py (comment on similarity or difference).
```

Do not include Input, Output, or Side effects fields: they restate the
signature and the convention that rules are pure. Do not describe what the
body visibly does. The docstring adds business meaning and citations the
code cannot carry.

## 3. Methodology module shape

Each module under `methbooks/methodologies/<provider>/<slug>.py` exposes
three public functions:

- `build_mock_data() -> pl.DataFrame`. Calls `build_base_universe()` and
  adds methbook-specific datapoints with random values.
- `get_data_dictionary() -> pl.DataFrame`. Reads the sibling CSV.
- `apply(df: pl.DataFrame) -> pl.DataFrame`. Calls rules in order.

Methbook-specific thresholds live as module-level constants. Conditions
are plain Python if/else. There is no `RULES` list.

`apply` ends with asserts on both technical and business or methodology
invariants (e.g. final row count within expected range, max per-security
weight respected, sector caps respected). Every assert message prints the
offending value.

## 4. Calendar variants

Separate modules per variant:
`methbooks/methodologies/<provider>/<slug>_quarterly.py`,
`methbooks/methodologies/<provider>/<slug>_semi_annual.py`. Reuse rules.
Each variant has its own
`methbooks/methodologies/<provider>/<slug>_data_dictionary.csv` if
datapoints diverge; otherwise variants share one file named after the
provider.

## 5. Data dictionary (per methbook)

Sibling CSV at
`methbooks/methodologies/<provider>/<slug>_data_dictionary.csv` with
header:

```
datapoint,description,source
```

One row per datapoint any rule in this methodology reads, plus the two
base columns `security_id` and `weight`. The `source` column points to
the markdown file and section, or to `methbooks/mock_universe.py` for the
base columns. The methodology module's `get_data_dictionary()` returns
the CSV as a polars DataFrame.

## 6. Worked example

A small linear methodology: one rule with a module-level threshold, one
methodology module with the three public functions, and the matching data
dictionary CSV.

`methbooks/rules/eligibility/exclude_zero_weight.py`:

```python
"""
Purpose: Drop rows whose weight is zero so downstream maths is well-defined.
Datapoints: weight.
Thresholds: MIN_WEIGHT (module constant, 0.0).
Source: methbooks/data/markdown/example.md section "Eligibility" near line 12.
See also: methbooks/rules/weighting/normalise_weights.py (downstream renormaliser).
"""
from __future__ import annotations

import polars as pl

MIN_WEIGHT = 0.0


def exclude_zero_weight(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("weight") > MIN_WEIGHT)
    assert "weight" in out.columns, f"weight column missing after filter: {out.columns}"
    assert out["weight"].min() > MIN_WEIGHT, (
        f"row with weight <= {MIN_WEIGHT} survived: {out['weight'].min()}"
    )
    return out
```

`methbooks/methodologies/example/simple.py`:

```python
"""Worked-example methodology: one eligibility rule over the base universe."""
from __future__ import annotations

from pathlib import Path

import polars as pl

from methbooks.mock_universe import build_base_universe
from methbooks.rules.eligibility.exclude_zero_weight import exclude_zero_weight

MIN_ROWS = 1
MAX_ROWS = 2000


def build_mock_data() -> pl.DataFrame:
    return build_base_universe()


def get_data_dictionary() -> pl.DataFrame:
    csv = Path(__file__).with_name("simple_data_dictionary.csv")
    return pl.read_csv(csv)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    out = exclude_zero_weight(df)
    assert MIN_ROWS <= out.height <= MAX_ROWS, (
        f"unexpected row count: {out.height} not in [{MIN_ROWS}, {MAX_ROWS}]"
    )
    assert float(out["weight"].sum()) > 0, (
        f"all weights zeroed: sum={out['weight'].sum()}"
    )
    return out
```

`methbooks/methodologies/example/simple_data_dictionary.csv`:

```
datapoint,description,source
security_id,Random 6-character security identifier.,methbooks/mock_universe.py
weight,Initial weight from base universe normalised to sum to 1.,methbooks/mock_universe.py
```

## 7. Graphify invocation

Run `graphify methbooks/` from r2 root. `methbooks/.graphifyignore`
(gitignore syntax) excludes `data/`, `pipeline/`, `CONVENTIONS.md`,
`AGENT_GUIDELINES.md`, and `examples/` so detection sees only code. With
zero docs, papers, or images in the corpus, graphify skips its semantic
pass and runs only AST extraction plus clustering (deterministic, no LLM
tokens).
