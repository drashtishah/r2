Start by Reading meth-pipeline/<slug>/<ts>/methbook_v1.json (planner's
draft) and meth-pipeline/<slug>/<ts>/input/markdown.md. Consult any
canonical examples under methbooks/examples/.

Fresh read; you have not seen the planner's reasoning. Your output is
an edited Methbook that conforms to the same schema. It is the
authoritative spec downstream agents will use.

Challenge the draft on these axes:

Atomicity and reuse
- Is each new_rule a single transformation? Split bundled rules.
- Ranking (ordered sort) and selection (cutoff pick) must be
  separate rules.
- Are reused_rules pointing at the right methbook and rule name? If
  a proposed new_rule already exists in another methbook (corporate
  events, a foundation doc, another index), convert it to a
  reused_rule.

Thresholds and categories
- Thresholds should be named module constants when feasible. An
  empty thresholds dict means the rule is binary.
- category is a closed enum: eligibility, ranking, selection,
  weighting, maintenance, event_handling, scoring. Reject any
  free-text category.

Datapoint metadata hygiene
- taxonomy must be the methodology's own domain classification
  (e.g., CBI restrictiveness level, MSCI ESG Rating seven-point
  scale), not a Python type like "boolean" or "numeric".
- semantic_threshold must be source wording, not paraphrase.
- Strip any definition_note claim that is not traceable to the
  markdown: no historical dates, no prior-threshold values, no
  effective-date assertions the source text does not contain.

Coverage and scoping
- Every numbered subsection and every distinct paragraph of a
  covered section must be accounted for in at least one rule's
  source.section, a slot's content, clarifications_needed, or
  scoped_out.
- Paragraphs about data-cutoff conventions, negative policies ("no
  deletions on X"), or review-cadence specifics must be explicitly
  addressed rather than silently dropped.
- Rules that govern between-review behavior (quarterly additions,
  inaction policies, event-driven adds/dels) belong in
  event_handling or selection, not scoped_out. Do not move
  transformations to scoped_out citing scheduling.

Slot completeness
- review_cadence.schedules must be populated with verbatim cadence
  phrasing from the source text if the source describes any
  rebalance cadence. An empty list when the source has cadence
  language is a defect.
- event_handling.defers_to should point to corporate_events when the
  source says "refer to MSCI Corporate Events Methodology book".
- external_references should have one DocRef per Methodology Set
  URL in the source.

Optionality and ambiguity
- If a slot's content is ambiguous in the source, move it to
  clarifications_needed rather than guessing. Silence in the source
  = slot unset; only populate from evidence.
- Slots you leave unset are fine. Do not invent content.

Edit any field of the Methbook. Output must match the Methbook JSON
schema exactly.
