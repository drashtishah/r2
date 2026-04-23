# Agent guidelines for methbooks

Karpathy-for-knowledge-work rules. Every agent that touches this tree
follows these.

1. Atomic. One rule, one transformation. Split anything whose purpose uses "and".
2. Minimal parameters. Default rule signature is def name(df) -> df. Add a kwarg
   only when the same rule is used at two or more different thresholds across
   methodologies. Otherwise thresholds live as module-level constants.
3. Reuse before create. Before drafting a rule, enumerate methbooks/rules/ and
   compare purposes. Cite the existing rule or justify why a new one is needed.
4. Cite the source. Every rule's docstring names the markdown file, section,
   and nearest line.
5. Flag ambiguity, do not guess. If the methodology text is unclear, emit a
   clarifications_needed note. Do not invent a threshold.
6. No invented datapoints. If a column is missing from the methbook's
   build_mock_data() and data_dictionary.csv, extend both in the same diff.
   Do not fabricate.
7. Surgical diff. Touch only methbooks/rules/, methbooks/methodologies/, and
   methbooks/mock_universe.py if a new base column is genuinely required.
8. One commit per rule. Plus one commit for the methodology and one for the
   data dictionary. Calendar variants are separate commits.
9. Assertions are invariants and debugging tools. Every rule ends with asserts
   covering both technical invariants and the business or methodology logic
   the rule claims. Every assert message names what went wrong and prints the
   offending value so a failure reads like a bug report.
10. Verifiers (deterministic for items 1 to 14, semantic for items 15 to 18)
    review diff, scope, docstring citations, and business-assert alignment.
    Neither re-plans. If the plan is wrong, that is planner or critique's
    failure mode.
