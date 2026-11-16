---
name: xlsx
description: Create, read, edit, and check Excel workbooks (.xlsx), including formulas, formatting, tables, and charts. Also use for spreadsheet-focused CSV/TSV conversion and existing .xlsm files.
---

# Excel workbooks in SEMOSS

Create or edit a real workbook with the requested sheet names, columns, units,
calculations, and formatting. Preserve a supplied workbook's conventions and start
from its latest saved version, including edits from the browser editor.

## Select the route

| Task | Route |
| --- | --- |
| Create a workbook | `openpyxl` in `ExecutePythonCode`; adapt [examples/create-workbook.py](examples/create-workbook.py) |
| Inspect or edit a workbook | Read [references/editing.md](references/editing.md) before saving |
| Formulas, cached values, or validation | Read [references/formulas.md](references/formulas.md) |
| Bulk tabular transformation | `pandas` for data; `openpyxl` for the final workbook layout |
| Quick content overview | `ReadFile` extracts Markdown; use native readers for cell addresses and formulas |

For example, `LoadSkill(skill_name="xlsx/references/formulas.md")`. Read only the
resources needed. Each file fits the default 8192-byte read; follow any returned
continuation offset. Packaged files under `.claude/skills/xlsx` are read-only.

## Execution and paths

Use managed `ExecutePythonCode`. `openpyxl`, `pandas`, and `xlrd` are declared in the
standard Python image; verify imports in the current deployment. Load `python`
when its runtime guidance is needed. No package installation or shell-based
LibreOffice workflow is assumed.

```python
from pathlib import Path
from smssutil import smss_get_runtime_var

root_value = smss_get_runtime_var("ROOT")
if not root_value:
    raise RuntimeError("ROOT is unavailable for this execution")
root = Path(root_value)
output = root / "requested-workbook.xlsx"
```

Save a workbook-specific program with `WriteFile`, then run its current source:

```python
source = root / "build-workbook.py"
exec(compile(source.read_text(encoding="utf-8"), str(source), "exec"), {})
```

The program imports its dependencies and resolves ROOT itself. Save and reopen
using absolute ROOT paths; use relative paths in room file tools. Do not use
`os.chdir()` or write binary data with `WriteFile`. If the executor or package is
unavailable, report the missing capability rather than inventing a tool.

## Workbook design

- Preserve requested formulas and source values. Use formulas for derived results
  that must update with inputs. Put assumptions in labeled cells and reference them.
- Keep numbers numeric and dates typed. Apply number formats for currencies,
  percentages, dates, units, and precision. Do not confuse a fraction with a percent.
- Use clear headers, readable column widths, wrapping, freeze panes, and filters
  when useful. Keep charts native and label their axes and units.
- Distinguish input cells and calculated cells; include a small legend when needed.
  For financial models, follow the user's or existing model's color/number conventions.
- Record source provenance and assumptions in visible notes, a source sheet, or cell
  comments. Spreadsheet comments are not currently exposed in the browser panel.
- For CSV/TSV, explicitly handle delimiter, encoding, leading-zero identifiers,
  missing values, and date parsing. Keep imported strings that start with `=` as
  literal text unless a formula was requested. CSV cannot retain Excel formatting,
  multiple sheets, or a workbook's formula/value model.

## Formula and preservation checks

`openpyxl` writes formulas but does not evaluate them. Recalculation flags only
request future calculation; they do not populate cached results. After saving a
formula workbook, follow the formulas reference to use the configured UnoServer
conversion service when suitable, inspect cached results, and independently check
important calculations. Never claim that opening the browser panel validates all
Excel formulas. Do not replace formulas with hardcoded totals to conceal a gap.

Use the bundled read-only `inspect_workbook` helper described in the references to
inventory formulas, stored errors, missing caches, and preservation risks. Its report
is not a formula evaluator or an OOXML schema validation. Cached values can be stale
even when all caches are present. A workbook with external links, macros, signatures,
pivots, or unsupported objects needs the preservation route in the editing reference.

## Completion

Reopen the saved workbook with `data_only=False` and verify sheet names, changed
cells, formulas, styles, tables, and charts relevant to the request. Read a second
copy with `data_only=True` to inspect calculated values; never save that second copy.
Compare important results against independent calculations and check unrelated
content after edits. Report inherited errors separately from new ones.

Confirm the output through `ListDirectory` or `GlobFiles`. For a collaboration room,
return `[Workbook](room://Workbook.xlsx)` using the verified relative path; encode
special characters per segment. The existing Excel panel supports viewing, editing,
Save, and Download. Outside a room use the platform's actual artifact link. State
whether recalculation and visual inspection ran and any unresolved gaps. There is
no `BuildXlsx` or `InspectXlsx` tool promised by this skill.
