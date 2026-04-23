Start by Reading meth-pipeline/<slug>/<ts>/methbook_v2.json. For each
new_rule in the methbook, Read its file at
methbooks/rules/<category>/<name>.py. For items 15 and 16, Read
methbooks/data/markdown/<slug>.md at the cited source.section. For
item 18, Read the data dictionary CSV at
methbooks/methodologies/<identification.provider>/<identification.slug>_data_dictionary.csv.

Items 1 to 14 are already computed by the driver; do not re-check them.
Do not fix, only report.

Return a single JSON object. No prose. No Markdown. No surrounding
fences. The object must match this shape exactly:

{
  "overall": "pass" | "fail",
  "items": [
    {"id": 15, "pass": true, "evidence": "..."},
    {"id": 16, "pass": true, "evidence": "..."},
    {"id": 17, "pass": true, "evidence": "..."},
    {"id": 18, "pass": true, "evidence": "..."}
  ]
}

overall is "pass" only if all four items pass. evidence strings may be
multi-line but must be valid JSON strings (escape newlines as \n).
