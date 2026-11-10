# Inspect before editing a workbook

Read the latest saved file, including browser edits, and write to a separate output
unless replacement was requested. Inspect sheet names and visibility, dimensions,
tables, merged ranges, formulas, defined names, comments, validations, charts, and
print settings relevant to the edit. Keep the existing design and input conventions.

Use the bundled read-only inventory before choosing a round-trip method:

```python
import runpy

helpers = runpy.run_path(str(root / ".claude/skills/xlsx/scripts/inspect_workbook.py"))
original_report = helpers["inspect_workbook"](root / "source.xlsx")
original_report
```

Resolve `root` as in the entry. Use the reported skill location if it was staged
elsewhere. The helper reads ZIP/XML without saving and counts stored errors, formula
anchors, and missing cached values across worksheets, including hidden sheets.
It caps location samples but returns complete counts. It flags some preservation
risks; an empty risk list is not a complete feature-compatibility guarantee.

## Ordinary `.xlsx` edits

```python
from openpyxl import load_workbook

source = root / "source.xlsx"
output = root / "revised.xlsx"
if output.exists():
    raise FileExistsError("Inspect the existing output before replacing it")
workbook = load_workbook(source, data_only=False, keep_links=True, rich_text=True)
sheet = workbook["Inputs"]  # Use names and cells established by inspection.
sheet["B4"] = 125
workbook.save(output)
workbook.close()
```

Reopen and verify the requested edit and unrelated content. Only replace a formula
when the request calls for that change; write to input cells for an assumption
change. For merged ranges, edit the top-left cell. Row/column insertion and deletion
do not reliably update every dependent formula, table, chart, or defined name;
adjust and verify dependencies explicitly. Copy styles deliberately when adding
cells. `pandas.to_excel` recreates sheets and is not a preservation editor.

Opening with `data_only=True` replaces formula expressions with cached values in
memory. Use that object only for reading and never save it. An `openpyxl` save can
clear cached results for formulas throughout the workbook, even if only one cell
changed. Follow [formulas.md](formulas.md) before delivery.

## Preserve advanced workbooks

- `.xlsm`: load with `keep_vba=True`, retain the `.xlsm` suffix, and preserve VBA
  bytes without executing them. This flag is not a guarantee for all embedded
  controls, signatures, or unsupported extensions.
- External links: `keep_links=True` retains link structures where supported, but
  a save can still discard formula caches. Keep original cached values for comparison;
  do not convert unavailable linked data to zeros or remove links.
- Drawings, pivots, slicers, connections, custom XML, or embedded objects: inspect
  the exact features before using `openpyxl`. It does not preserve every object.
  Ordinary supported charts are different from arbitrary drawing shapes.
- Signatures and encryption/protection: do not silently invalidate signatures,
  strip protection, or execute macros. Use an appropriate authorized client when
  fidelity cannot be preserved by available tools.

For an isolated edit in a feature-rich file, a targeted OOXML change can copy every
untouched ZIP member byte-for-byte. Use `zipfile` without extracting uploaded paths;
use `lxml` with DTDs, network access, and entity expansion disabled. Identify sheets
through `xl/workbook.xml` and its relationships, not an assumed `sheet1.xml` order.
Preserve cell style IDs and types; respect shared-string indexes. Editing one shared
string affects every referencing cell, so allocate/repoint a string for an isolated
change. Clear any affected formula cache and mark calculation pending. Never retain
stale results and present them as current. Avoid rewriting an array/shared-formula
region without understanding its anchor and dependencies.

For fidelity-sensitive workbooks where safe recalculation is unavailable, retain
formulas and original package features and disclose that current results are
unverified. Do not force a LibreOffice round trip just to get a clean cache report.

## Other formats

`xlrd` can read legacy `.xls` data but does not provide a formatting-preserving editor.
Use the configured conversion service to create a separate `.xlsx` when conversion
is acceptable, then compare content. Keep `.xltx` template semantics (`workbook.template`)
consistent with the output extension. CSV/TSV carries one text table; document any
loss of multi-sheet structure, formulas, formatting, or type precision during conversion.
