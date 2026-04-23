Start by Reading meth-pipeline/<slug>/<ts>/plan_v2.json. For each new
rule in the plan, Read its file at the path the plan specifies. For
items 15 and 16, Read methbooks/data/markdown/<slug>.md at the cited
section. For item 18, Read the data dictionary CSV named in the plan.

Items 1 to 14 are already computed by the driver; do not re-check them.
Do not fix, only report.

For each of items 15 to 18 produce {id, pass, evidence}. Return
aggregate pass or fail.
