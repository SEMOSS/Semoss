# Word and Excel system skills

`docx` and `xlsx` follow the existing `pptx` packaging convention: a
`project/platform__<name>.smss` registration and a public skill directory containing
`SKILL.md`, focused references, a creation example, and a small format-specific helper.
Each skill resource fits a default 8192-byte `LoadSkill` read.

Both are registered in `SystemDefaultEngines` and added to collaboration-room defaults
and the collaboration system agent. Other agents retain their existing skill sets;
users can attach these system skills through the normal selection flow. Deploy the
Java catalog changes, `.smss` files, and public directories together and restart the
backend so its normal system-project registration loads them. No live database or
agent workspace is manually modified by this source change.

## Platform adaptation

| Capability | SEMOSS implementation |
| --- | --- |
| Word creation | Managed Python with `python-docx`; native editable document elements |
| Word editing | Native object edits; a guarded cross-run text helper preserves untouched ZIP members |
| Word review features | `python-docx` comments; scoped OOXML guidance for tracked changes, with explicit validation limits |
| Excel creation/editing | Managed Python with `openpyxl`; optional `pandas` for data transformation |
| Formula checks | Read-only ZIP/XML inventory plus independent numeric checks; optional UnoServer round trip |
| Rendering/conversion | Existing browser panels and `ConvertFileUnoserver` through `Insight.run_pixel` |
| Execution/files | `ExecutePythonCode`, per-execution ROOT, saved generators, room file verification |
| Delivery | Verified `room://` Markdown links open the current Word/Excel panels |

The Word browser panel currently hides comments and tracked-change actions. The
Excel panel hides comments, pivots, slicers, and some other commands. The skills do
not treat those hidden controls as supported review workflows or promise native
Office fidelity from the viewers. No frontend dependency change is needed.

## Dependencies checked on November 2, 2026

The baseline was checked in `py/install_config/pyproject.toml`; installed versions
were checked using the interpreter configured by this instance's `PYTHONHOME`.

| Package/service | Observed availability | Purpose / decision |
| --- | --- | --- |
| `python-docx` 1.2.0 | Explicit baseline dependency, installed | Word creation, native inspection/editing, anchored comments |
| `openpyxl` 3.1.5 | Explicit baseline dependency, installed | Excel cells, formulas, styles, tables, charts, and native readback |
| `pandas` 3.0.6 | Explicit baseline dependency, installed | Optional bulk data import/cleanup; not a workbook preservation editor |
| `lxml` 6.1.3 | Explicit baseline dependency, installed | Targeted Word XML edits and read-only workbook inspection |
| `xlrd` 2.0.2 | Explicit baseline dependency, installed | Optional legacy `.xls` data reading |
| `docling` 2.141.0 | Explicit baseline dependency, installed | Existing platform Markdown extraction |
| `Pillow` 13.0.0 | Installed transitively | Optional image insertion; not needed by the bundled examples/helpers |
| UnoServer / LibreOffice | Existing configured service and conversion reactor | PDF/legacy conversion and candidate Excel recalculation; validate results per deployment |

**No additional package is required for these adaptations.** Packages that would
otherwise be needed for the upstream execution approach are deliberately replaced
by existing platform capabilities:

| Upstream dependency | Current status | Why it appears upstream / SEMOSS replacement |
| --- | --- | --- |
| `docx` (npm) | Absent from curated Node dependencies | Word authoring in JavaScript; use installed `python-docx` instead |
| `pandoc` | Not found on this host | Word-to-Markdown conversion; use platform Docling/native reads |
| `markitdown` | Not installed in configured Python | Office text extraction; use existing `ReadFile`/Docling |
| Local LibreOffice and Poppler `pdftoppm` | Not a managed-agent runtime contract | PDF/image rendering; use configured UnoServer and existing viewers |

Codex-local renderer binaries found on the developer machine do not establish their
availability inside SEMOSS or in a deployed image. No npm/pip installations or new
conversion service were added. If a deployment lacks the declared Python packages,
restore its standard environment. If it lacks UnoServer, creation/editing remains
available but server-side conversion/recalculation is unverified until that service
is configured. `openpyxl` alone is not an Excel calculation engine.

## Source review and provenance

The requested references were reviewed for capabilities and runtime assumptions:

- [Anthropic Word skill](https://github.com/anthropics/skills/tree/main/skills/docx)
- [AgenticSkills Word listing](https://agenticskills.io/skills/docx)
- [Anthropic Excel skill](https://github.com/anthropics/skills/blob/main/skills/xlsx/SKILL.md)

AgenticSkills identifies its Word listing as synced from Anthropic, rather than a
separate implementation. Its repository-level Apache-2.0 label does not match the
document skills' own `license: Proprietary` declarations and restrictive
[Word](https://github.com/anthropics/skills/blob/main/skills/docx/LICENSE.txt) and
[Excel](https://github.com/anthropics/skills/blob/main/skills/xlsx/LICENSE.txt) license
files. These new SEMOSS instructions, examples, and helpers are independently authored;
no upstream skill text, scripts, schemas, or assets are vendored. The existing PPTX
package's upstream content was not changed by this work.

API behavior was checked against [python-docx's API](https://python-docx.readthedocs.io/en/latest/api/document.html),
[openpyxl's loading tutorial](https://openpyxl.readthedocs.io/en/stable/tutorial.html),
and [its formula documentation](https://openpyxl.readthedocs.io/en/stable/simple_formulae.html).

## Validation

Run `py/install_config/.venv/bin/python -m unittest discover -s py/testing -p test_office_skills.py`.
The tests stage the actual skill resources, run both authoring examples with ROOT
different from the process directory, exercise native Excel edits and guarded Word
edits, verify untouched Word package members, and check formula caches/errors and
preservation-risk reporting. Java catalog coverage is in `SystemDefaultEnginesUnitTests`.
Helpers do not claim XSD validity, a full tracked-change engine, formula evaluation,
or proof of visual rendering. Conversion success still requires independent checks
of cached results and feature preservation before delivery.

Verified locally: all 8 Python skill tests and 22 selected Java tests passed, and
both skill entrypoints passed the skill frontmatter validator. The configured
UnoServer service converted the synthetic Word example to a one-page PDF and
round-tripped the synthetic workbook to XLSX. The three formulas remained formulas
and their cached values were 700, 300, and 1000, with zero stored errors or missing
caches; both sheets, the table, and the chart survived. This checks the actual
conversion service using synthetic data, not an end-to-end managed Pixel invocation
or a browser editing session. The backend was not restarted for this change.
