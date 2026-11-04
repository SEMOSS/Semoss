# Formulas and saved results

An Excel cell can store a formula and a cached result. `openpyxl` writes the former
without calculating the latter. `calcMode="auto"`/`fullCalcOnLoad=True` are requests
to an application, not calculation evidence. A browser may implement only a subset
of Excel; loading a workbook is not proof that every result is valid or saved.

Use formula strings with English function names and comma-separated arguments.
Quote sheet names correctly (use `openpyxl.utils.quote_sheetname`). Keep assumptions
in referenced input cells, use appropriate absolute references, and check ranges,
units, dates, rounding, and denominator behavior. Prefer formulas supported by the
chosen calculation engine. Do not silently replace a requested function; newer
functions, dynamic arrays, data tables, and external links need explicit verification.

## Inspect stored results

```python
import runpy

inspect_workbook = runpy.run_path(
    str(root / ".claude/skills/xlsx/scripts/inspect_workbook.py")
)["inspect_workbook"]
report = inspect_workbook(root / "Cost plan.xlsx")
report
```

The report has per-sheet and total formula/error/cache counts, sampled locations,
preservation flags, and `calculation_verified=False`. This flag intentionally stays
false: the helper cannot evaluate formulas or prove that present results are fresh.
`cache_status="missing"` needs calculation; `present_unverified` needs freshness and
correctness checks; `not_applicable` means no formula anchors were found. A formula
that returns an empty string can have a valid empty string cache. Stored error cells
remain errors even when every cache is present. The helper supports transitional
OOXML and limits inspection to 256 MiB uncompressed; report other formats/size limits.

## Recalculate through the existing conversion service

For an ordinary workbook whose features can survive LibreOffice, use SEMOSS's
configured `ConvertFileUnoserver` reactor. Create a separate result first. In the
current room's insight root, after confirming the source and an unused output path:

```python
from semoss import Insight

conversion = Insight().run_pixel(
    'ConvertFileUnoserver(filePath=["Cost plan.xlsx"], format=["xlsx"], '
    'outputFilePath=["Cost plan-calculated.xlsx"], space=["insight"]);'
)
conversion
```

This asks the configured service to open/export a workbook. Do not equate conversion
success with recalculation success. Inspect `pixelReturn` for errors, confirm the
returned file, run `inspect_workbook` again, and inspect actual cached values. The
service version and supported formulas vary by deployment. Do not run a local
`recalc.py`, install LibreOffice, or send files to an unrelated service.

Pixel paths are relative to the selected asset space, not necessarily Python ROOT.
For project/user/subdirectory targets, use the known `space` and include the path
relative to that asset root. Do not guess a scope from a filename or pass an absolute
server path. A missing/failed conversion capability is a reported verification gap.

Do not round-trip externally linked, macro-enabled, signed, or unsupported-feature
workbooks automatically. Converters can remove features or alter links/formulas.
Use the preservation guidance in [editing.md](editing.md). Keep the original and
compare formulas, sheets, tables, charts, names, and other requested features in a
conversion candidate before selecting it as the deliverable.

## Verify calculation, then deliver

1. Reopen the candidate with `data_only=False` and confirm formula expressions or
   equivalent references survived. A calculated literal replacing a formula is a
   regression, even if its value is correct.
2. Reopen a separate read-only copy with `data_only=True`. Inspect important results
   and compare them with independent calculations from the source data. Check all
   stored errors using the helper; inspect affected dependencies, not just totals.
3. For the example workbook, the cached values of `D2`, `D3`, and `D4` must be 700,
   300, and 1000. Actual tasks need expectations derived from their own inputs.
4. Fix introduced formula errors in the authoring/editing program, resave, and repeat
   calculation checks. Compare errors to the original before calling them inherited.
   Missing caches, changed features, or unsupported formulas are not a passed review.
5. Once the candidate is verified, copy its bytes to the user's requested final
   filename if needed, preserving any original that must remain. Do not save it
   again with `openpyxl` after calculation: that clears cached results again.

If a suitable calculator is unavailable, deliver the editable workbook with an
explicit note that formula results remain unverified and may be blank or stale in
the preview. Do not fabricate cache values or advertise a calculation pass. If the
user requires verified results, identify that requirement as unfinished.

When browser tools are exposed, inspect formatting, widths, chart labels, and visible
values in the Excel panel. Record actual coverage separately from the formula checks.
