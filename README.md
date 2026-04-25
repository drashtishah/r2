# r2

Encode index methodology PDFs as Python rule functions, build a graph over
that code, and let agents answer questions about the methodologies with
citations back to the source page.

## AST graphs as knowledge graphs

Each methodology becomes a small set of Python rule functions. `make graph`
walks that code and produces a graph: nodes are files and functions, edges
are contains, calls, and imports between them. The structure of the rules
is the structure of the methodology, so the graph is the knowledge graph.

## Schema in brief

A methbook describes one methodology. It has identification, optional
sections for universe, eligibility, scoring, selection, weighting, event
handling, and review cadence, and a list of rules. Each rule names what it
does, the datapoints it reads, the thresholds it uses, what must be true
after it runs, and where in the source it came from (page and line). Every
rule that comes from a document with page footers carries a page, so a
human can flip to that page and verify.

## Pipeline

- `setup`: convert the PDF to markdown and create a run directory.
- `planner`: read the markdown and write a draft methbook.
- `critique`: re-read the source and refine the draft.
- `commit_plan`: write the final methbook as a sidecar JSON; auto-fill page numbers.
- `implementer`: write Python rule functions and the methodology module.
- `deterministic`: mechanical checks (paths, asserts, data dictionary).
- `semantic`: LLM checks on business logic and ambiguity.
- `rules_index`: map methodologies to the rules they use.
- `enrich_graph`: fold those import edges into the AST graph.

## What you can ask

- Which methodologies exclude companies based on controversies?
- What is the same and what is different between two given methodologies?
- Which methodologies reuse a given rule, and on which page do they cite it?

Every answer cites the source page, so verification is one click away.

## Run it

- `make methbook SRC=<pdf-path>` runs the full pipeline end to end.
- `make graph` refreshes the graph.
- `make test` runs the test suite.

## Next

A query agent that consumes the graph, the plan sidecars, and the rule
code to answer the questions above is the next step. Not yet built.
