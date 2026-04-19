# Note body templates

One template per type. Keep bodies short; notes are reference material, not essays.

## event

```
---
type: event
date: 2026-04-18
domain: <domain>
source: <url or citation>
---

# <simple sentence, no jargon>

<one or two sentences stating what happened, with ≥1 [[specific-link]] woven in>.
```

## specific

```
---
type: specific
domain: <domain>
---

# <concept name>

<definition in one or two sentences; every jargon term either defined inline or linked>. <Link to [[abstract-concept]] for the underlying idea>.
```

Hub specifics (which federate child specifics) may skip the abstract link. Declare hub status when presenting the note.

## abstract

```
---
type: abstract
---

# <plain-English concept>

<one or two sentences defining the concept in general terms; no domain examples, no dates>.
```

## mechanism

```
---
type: mechanism
---

# <causal claim in a short sentence>

<Single sentence stating the causal relationship between [[cause]] and [[effect]]>. <One sentence of evidence: an event link, a citation, or a derivation>.
```

### Optional ASCII diagram (mechanism example)

When a causal chain or ordered process is easier to see than read, add one spare ASCII diagram inside a fenced code block after the prose. Prose first, diagram second.

```
[[cause]] ──► intermediate ──► [[effect]]
```

Guidelines:

- Keep it under ~6 lines.
- Use only `─ │ ┌ ┐ └ ┘ ├ ┤ ┬ ┴ ┼ ►` or plain ASCII `- | + > ^`.
- Wiki-link targets inside diagrams still count as real links.
- Skip the diagram if prose is already clear.
