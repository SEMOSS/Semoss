# Create and verify documents

## Establish the deliverable

Use the requested audience, format, scope, filename, and source material. For ordinary
layout choices, choose a readable design and continue. Ask only for a missing decision
that changes the substance or prevents useful work. Preserve a supplied template and
the owner's existing edits. Separate factual source content from analysis and proposals.

Load the separate `python` skill for execution and package guidance. For presentation
work, load `pptx` when it is available. Use an installed format-appropriate library:
for example `python-docx` for Word, `openpyxl` for Excel, and an available PDF writer
or conversion tool for PDF.
Check availability rather than assuming a package, renderer, or converter is enabled.
When a report uses an existing document as evidence, read it with
`from smssutil import get_document_markdown` before drafting. For output verification,
its Markdown can check expected text; still reopen the native format for formulas,
structure and other required details, and inspect rendered output for layout.

For Python outputs, resolve `Path(smss_get_runtime_var("ROOT")) / filename` on each
execution and pass that path to the library's save and reopen methods. Python's
current directory may differ from `ROOT`; a bare `doc.save("summary.docx")` can put
the document outside the room. Load
[files/write-and-deliver.md](../files/write-and-deliver.md) for the complete path,
verification, recovery, and handoff rules.

## Build content the recipient can use

- Word/report: lead with the requested answer or recommendation, organize related
  evidence, use proper headings and tables, and retain sources for material claims.
- Spreadsheet: include clear column names, units, typed numbers/dates, and readable
  widths. Distinguish inputs from calculations. Prefer formulas for values that should
  update when inputs change, and state when formula results have not been recalculated.
- PDF: check pagination, margins, table wrapping, image quality, and the reader's
  ability to find the answer. Use an available rendering/conversion route if needed.
- Chart: label axes and units, identify the time period and source, and use source
  values. Do not manufacture data to fill a chart or convert missing values to zero.

Keep documents editable when that is requested. Avoid rendering an entire editable
report or spreadsheet as images. For an existing file, edit a copy unless replacement
is explicitly requested; preserve unrelated pages, sheets, content, and formatting.

## Check the saved output independently

| Check | Evidence to collect |
| --- | --- |
| File saved | Room file tools confirm the exact filename and expected nonzero size |
| Format readable | Reopen using a format-appropriate reader; check a PDF's pages or Office package |
| Content complete | Required headings, sections, tables, sheets, or slide count |
| Calculations correct | Independently recomputed totals, units, rounding, missing-data handling |
| Sources accurate | Key claims match the cited source; proposals are labeled |
| Edits scoped | Requested changes present; unrelated material preserved |
| Layout usable | Rendered pages/slides inspected when an available tool supports it |

Creation and verification are separate steps. Reopening checks structure and content;
it does not prove visual layout. If rendering or recalculation is unavailable, name
that limitation instead of claiming a visual or formula-result pass.

For presentations, follow the `pptx` execution and review contract when that skill is
available. Loading the skill does not activate a managed author/reviewer workflow. When `BuildPptx` is
exposed, its result owns the saved artifact and review outcome. Preserve failed,
incomplete, or inconclusive review status accurately.

## Deliver what was saved

Use [files/write-and-deliver.md](../files/write-and-deliver.md). Give the actual
output as a verified Markdown link such as `[summary.docx](room://summary.docx)`,
or the exact artifact link returned by the platform, plus material
verification limits. If the request also includes emailing the output, finish the
file checks before attaching it through the draft workflow.
