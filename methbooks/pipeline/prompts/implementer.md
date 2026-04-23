Start by Reading meth-pipeline/<slug>/<ts>/plan_v2.json (the
authoritative plan).

For each new rule, Write the file at
methbooks/rules/<category>/<name>.py exactly as the plan specifies;
after each write, commit with Bash (git add <path> && git commit
-m "..."). For reused rules, Read the existing file at the path from
reused_rules to know the signature before calling them from apply.

The methodology module uses build_mock_data to extend the base universe
with each non-base datapoint in the dictionary (using the distribution
hint), get_data_dictionary to read the sibling CSV, and apply to call
rules in composition_order ending with final_asserts.

One commit per rule (bundling a new __init__.py with the first rule in
that category), one for the methodology module, one for the data
dictionary.

Do not write outside methbooks/rules/, methbooks/methodologies/, or
methbooks/mock_universe.py; the deterministic verifier will fail the
run if you do.
