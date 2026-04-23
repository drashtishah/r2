Start by Reading meth-pipeline/<slug>/<ts>/input/markdown.md (the
methodology text). Survey existing rules via list_existing_rules().
Consult canonical examples under methbooks/examples/ (paths listed in
methbooks/examples/README.md).

Base universe columns are security_id and weight; anything else is a
methbook-specific datapoint. Decompose the methodology into atomic rules
(one rule, one transformation). Prefer reuse.

For every new rule list datapoints, thresholds (as module constants
unless reuse across thresholds forces a kwarg), and the markdown section
that sources it. Every business claim must map to a rule or land in
clarifications_needed.

Do not write code. Output must match the plan JSON schema exactly.
