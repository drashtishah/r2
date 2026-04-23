Start by Reading meth-pipeline/<slug>/<ts>/input/markdown.md (the
source methodology text). Survey existing rules via
list_existing_rules(). Consult canonical examples under
methbooks/examples/ if present.

Your output is a single Methbook that conforms to the schema. The
schema is the same for every MSCI doc: index families (Selection,
Quality, Climate Action, ...) and foundation docs (Corporate Events,
GIMI, GICS, Index Calculation, Index Policies, Fundamental Data)
alike. They differ only in which slots they populate.

Every slot except identification is optional. If the source is silent
on a slot, leave it unset. Do not fabricate content to fill a slot.
If the source is ambiguous about a slot, add a clarifications_needed
entry quoting the ambiguous text and describing the concern; do not
guess.

Identification

- provider: "MSCI"
- family: the index family or foundation-doc name (e.g., "Selection",
  "Quality", "Corporate Events", "GIMI")
- slug: lowercase, snake_case, derived from family
- version_date: the publication date stamped on the PDF (YYYY-MM-DD)
- index_type: one of esg_selection, esg_screen, climate, factor,
  overlay, reference, other. Reference = foundation docs that do not
  define a specific index family (Corporate Events, GIMI, GICS,
  Index Calculation, Index Policies, Fundamental Data).
- base_methbook: set for variants only. A variant like "Extended
  Climate Action" points to its parent's slug and inherits; the
  variant methbook only lists the DELTA from the parent.
- regulatory_framework: only when the source doc names one explicitly
  (EU Delegated Regulation 2020/1818 for CTB/PAB, UN treaties for XCW).

Rules (new_rules and reused_rules)

- new_rules: atomic transformations defined in this doc. One rule,
  one transformation. Ranking (ordered sort) and selection (cutoff
  pick) are separate rules.
- reused_rules: rules defined in a different methbook, referenced by
  {methbook: slug, name: rule_name, justification}. Before proposing
  a new_rule, check whether the same rule already lives in another
  methbook (e.g., spin_off_treatment in corporate_events).
- rule.category is a closed enum: eligibility, ranking, selection,
  weighting, maintenance, event_handling, scoring.
- rule.thresholds: dict of named constants. Empty dict means the
  rule is binary/flag-based. For CBI-style rules with revenue
  thresholds, capture them as named constants.
- rule.canonical_concept (optional): stable slug identifying the
  underlying concept across methbooks. Populate when this rule
  matches a concept that another methbook likely also uses, even if
  the source phrasing differs (e.g., "Controversies Score of 3 or
  above" and "Red Flag Controversies Score = 0 excluded" are both
  msci_controversies_score_eligibility with different thresholds).
  Use snake_case. Leave null if you are not confident the concept
  recurs; downstream cross-methbook diffing will fall back to name
  matching.

Event-handling vs unrated-treatment boundary: event_handling rules
govern what happens when something CHANGES (spin-offs, M&A, rating
downgrades between reviews, PAF-timed price adjustments).
unrated_treatment governs what happens when DATA IS MISSING for a
security (no MSCI ESG Rating, no Controversies Score). If in doubt,
"caused by an event" → event_handling; "caused by missing data" →
unrated_treatment.

Slot-specific guidance

- universe: applicable_universe is a DocRef to the universe-defining
  methbook (usually gimi). reference_index only for overlay indexes.
- data_requirements.mandatory_products: DocRefs to the MSCI data
  products the methodology requires (ESG Ratings, Controversies,
  BISR, Climate Metrics, Fundamental Data, GICS).
- scoring: populate only for score-producing methbooks. Capture
  variables, normalization (z_score_global / z_score_sector_relative /
  quartile / ...), aggregation, any transformation or conditional
  adjustments.
- selection.method: common values coverage_cutoff, fixed_number,
  optimization, screen_only, coverage_plus_score. If the source
  describes something that does not match, use a descriptive
  snake_case label and put details in content.
- weighting.method: common values ff_mcap, parent_proportional,
  score_tilt, optimizer_output, ff_mcap_with_caps. Same rule: if
  unclear, surface to clarifications_needed rather than guessing.
- review_cadence.schedules: one entry per distinct cadence (annual,
  semi-annual, quarterly). Capture months and data_cutoff verbatim.
- event_handling.defers_to: DocRef to corporate_events if the doc
  says "refer to MSCI Corporate Events Methodology book". This is a
  structured pointer, not scoping-out.
- event_handling.intra_review_invariants: negative policies ("no
  deletion between reviews on ESG downgrade") belong here.
- composition_order: ordered list of new_rule names that run at
  apply() time, in the order they execute on the DataFrame. Include
  only rules that run during a standard rebalance; event_handling
  rules and quarterly-only rules stay out of composition_order.
- final_asserts: post-pipeline invariant assertions that must hold
  on the final apply() output (e.g., weights sum to 1, no rating
  below threshold).
- external_references: every Methodology Set URL in the source
  becomes a DocRef. The methbook slug is the target doc's slug
  (corporate_events, gimi, gics, fundamental_data,
  index_calculation, index_policies).

Mock datapoints

- Every datapoint consumed by a rule needs a mock_datapoints entry.
- For datapoints whose definition lives in an appendix or introduces
  a semantic threshold, fill source_section, semantic_threshold
  (source wording verbatim), taxonomy (the methodology's own
  classification, e.g., CBI restrictiveness level or rating-scale
  name, not the Python type), and definition_notes (edge-case
  carve-outs).
- canonical_concept (optional): stable slug identifying the
  underlying datapoint across methbooks (e.g., msci_esg_rating,
  msci_controversies_score, gics_sector). Use for datapoints you
  expect to recur in other MSCI methbooks.
- Every claim must be traceable to the markdown. Do not introduce
  historical dates, prior-threshold values, effective-dates, or any
  metadata the source text does not contain.

Coverage and scoping

- Every numbered subsection (2.x, 3.x, Appendices) of the source
  must appear in at least one of: a rule's source.section, a slot's
  content (universe, review_cadence, ...), clarifications_needed, or
  scoped_out.
- scoped_out is for sections that describe no transformation at all
  (document metadata, historical changelog, external URL lists).
  Scheduling alone is not a reason to scope out a transformation;
  between-review behavior (quarterly additions, inaction policies,
  event-driven adds/dels) belongs in event_handling or selection.

Do not write code. Output must match the Methbook JSON schema exactly.
