# Edit the saved Word document

Read the current file and preserve a source copy. Identify the exact paragraph,
table cell, header/footer, or image affected. A Markdown extraction is useful for
reading but omits formatting and may omit revisions, text boxes, or other parts.

## Native object edits

For ordinary structure and formatting, open `Document(source)` with `python-docx`.
Change only the requested objects, save to a new ROOT path, and reopen. Assigning
`paragraph.text` or `cell.text` rebuilds their contents and can remove run formatting,
links, fields, or drawings. Prefer a specific `run.text` only after inspecting that
run's content. Even assigning `run.text` can replace non-text children in that run.

Inspect tables recursively and inspect each section's headers/footers when relevant.
`document.paragraphs` is not all text in the package. A new document generated from
extracted text is not a preservation edit. `python-docx` is not a complete fidelity
guarantee for every Word extension; check the package's changed parts for complex files.

## Literal text spanning ordinary runs

The bundled helper changes only the selected XML member, preserving all other ZIP
members' bytes. It handles text split across differently formatted runs without
flattening the paragraph. The new text inherits the first replaced character's
formatting. Use it when that is the intended style.

```python
import runpy

helpers = runpy.run_path(str(root / ".claude/skills/docx/scripts/replace_text.py"))
report = helpers["replace_text"](
    root / "source.docx", root / "revised.docx",
    "Previous project title", "Updated project title", expected=1,
)
report
```

Resolve `root` using the entry's runtime-path contract. If the skill was loaded from
another staging location, use its reported path. `expected` is the number established
by inspection, not a value to increase until an unexpected replacement succeeds.
For a header/footer, pass its actual package member as `part="word/header1.xml"`,
after inspecting the section relationship. The helper refuses existing output
files, digital signatures, and matching paragraphs containing links, revisions,
fields, bookmarks, drawings, or controls. A refusal leaves the original untouched;
inspect and use a targeted native/XML edit instead of removing those structures.

Afterward, reopen and check new text and run formatting. Compare each untouched ZIP
member with the source. The helper is a normal edit, not a redlining implementation.

## Complex package edits

Use `zipfile` and `lxml` only for the inspected OOXML feature. Read members directly
without extracting arbitrary uploaded paths. Parse with entity resolution and
network access disabled; reject DTDs. Keep whitespace and namespace declarations,
and avoid pretty-printing the complete document. Copy unrelated members unchanged.
Preserve relationships, content types, styles, numbering, comments, and embedded media.

Find images through the drawing's relationship ID; filenames alone do not identify
the correct instance. A shared media part can affect several visible images, so
duplicate and relink it when only one occurrence should change. Preserve the image's
extent and aspect ratio unless resizing was requested.

Do not silently strip a signature, content control, macro, or revision to make an
edit work. Report unsupported fidelity requirements and keep the original. For
comments and tracked changes use [review.md](review.md). For layout checks and
legacy formats use [verification.md](verification.md).
