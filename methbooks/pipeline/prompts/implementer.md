Start by Reading meth-pipeline/<slug>/<ts>/methbook_v2.json (the
authoritative methbook). Key fields you will use:

- identification.provider and identification.slug: use these
  verbatim for output paths. Do NOT derive paths from the run-dir
  slug in the meth-pipeline path above; that is the raw PDF stem
  and is typically long and version-stamped. identification.slug is
  the human-friendly methbook identifier (e.g., "selection").
- new_rules: each rule's category, name, datapoints, thresholds,
  source, asserts
- reused_rules: {methbook, name, justification} for cross-methbook
  reuse
- composition_order: ordered list of new_rule names to call in
  apply(), left to right
- final_asserts: post-pipeline invariants for apply()
- mock_datapoints: for build_mock_data and the data dictionary CSV

For each entry in new_rules, Write the file at
methbooks/rules/{rule.category}/{rule.name}.py exactly as the
methbook specifies; after each write, commit with Bash (git add
<path> && git commit -m "..."). The file must define exactly one
public function named {rule.name} with signature
def {rule.name}(df) -> DataFrame and at least one messaged assert
inside that function. Private helpers with a leading underscore
are allowed for readability (e.g., _select_sector inside
select_by_sector_coverage_target).

Commits can bundle related rules within the same category if they
are co-introduced (e.g., four event_handling rules in one commit).
At minimum, emit one rule commit plus a separate methodology commit.

For each entry in reused_rules, Read the existing rule file in the
source methbook's tree to confirm the signature before calling it
from apply().

The methodology module path is
methbooks/methodologies/{identification.provider}/{identification.slug}.py.
It defines three functions:
- build_mock_data extends the base universe (security_id, weight) with
  each non-base datapoint in the dictionary using the distribution hint.
- get_data_dictionary reads the sibling CSV
  methbooks/methodologies/{identification.provider}/{identification.slug}_data_dictionary.csv.
- apply calls the rules named in composition_order in order, ending
  with the final_asserts statements verbatim.

One commit per rule (bundling a new __init__.py with the first rule in
that category), one for the methodology module, one for the data
dictionary.

Do not write outside methbooks/rules/, methbooks/methodologies/, or
methbooks/mock_universe.py; the deterministic verifier will fail the
run if you do.

Before finishing, run these three checks from the repo root via Bash
and fix anything non-zero, then rerun:

1. `make typecheck`
2. `uv run python -c "from methbooks.methodologies.{provider}.{slug} import apply, build_mock_data; apply(build_mock_data())"`
3. `uv run python -m methbooks.pipeline.deterministic --run-dir meth-pipeline/<slug>/<ts>`

Substitute {provider}/{slug} from identification, and <slug>/<ts> from
the run_dir above. Only stop when all three exit zero. Commit any fixes
you make during this loop on the current branch alongside your other
commits. The deterministic check writes its report to run_dir; read it
to see which item failed.
