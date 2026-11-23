---
name: docx
description: Create, read, and edit Word documents (.docx), preserving formatting and existing content. Use for Word reports, letters, templates, tables, comments, and tracked-change requests.
---

# Word documents in SEMOSS

Produce an editable Word file using the user's content, filename, template, and
design direction. Start edits from the latest saved document, including changes
made in the browser editor. Preserve the source unless replacement was requested.

## Choose the work

| Task | Route |
| --- | --- |
| Create a document | `python-docx` in `ExecutePythonCode`; adapt [examples/create-document.py](examples/create-document.py) |
| Read text and tables | `ReadFile` for extracted Markdown; `docx.Document` for paragraph/table/run inspection |
| Edit an existing file | Read [references/editing.md](references/editing.md); choose native objects or a targeted XML edit |
| Comments or tracked changes | Read [references/review.md](references/review.md); these require different document structures |
| Check layout or convert legacy `.doc` | Read [references/verification.md](references/verification.md) |

Load only the needed resources, for example
`LoadSkill(skill_name="docx/references/editing.md")`. Entry, references, and examples
fit the default 8192-byte read. Follow any continuation offset reported by the tool.
Keep packaged files under `.claude/skills/docx` read-only.

## Managed execution

Use `ExecutePythonCode` and the installed `python-docx` distribution (import `docx`).
For detailed execution rules, load `python` when available. Resolve paths on every
execution; the Python process directory is not necessarily the room directory:

```python
from pathlib import Path
from smssutil import smss_get_runtime_var

root_value = smss_get_runtime_var("ROOT")
if not root_value:
    raise RuntimeError("ROOT is unavailable for this execution")
root = Path(root_value)
output = root / "requested-document.docx"
```

Save a document-specific generator using `WriteFile`, then execute its current source:

```python
source = root / "build-document.py"
exec(compile(source.read_text(encoding="utf-8"), str(source), "exec"), {})
```

The saved program must resolve its own ROOT and import its dependencies. Reuse a
generator only for a new document that has no subsequent user edits. For later edits,
open the latest `.docx`. `WriteFile` writes text, so use the document library to save
the binary file. Do not install packages or launch shell conversion commands. If
Python or a required import is unavailable, report that dependency and the work
that remains; the presence of a skill does not enable an executor.

## Authoring decisions

Use native paragraphs, headings, lists, tables, and images so the result remains
editable. Set page size and margins deliberately. Use styles for a consistent type
hierarchy and spacing; match an existing template before changing its design. Use
real list styles, table column widths that fit the usable page, and image dimensions
that preserve aspect ratio. Keep headings with their following text and check table
splits. A field for page numbers or a table of contents needs a renderer to update
its displayed result; inserting the field alone is not proof of a correct result.

For supplied templates, inspect sections, headers, footers, tables, and styles as
well as body paragraphs. `.dotx` is a template content type, not a `.docx` renamed:
materialize a document through an available converter or deliberately update the
package's main content type on a copy. Never silently discard macros or signatures.

## Verify and deliver

Reopen the exact saved path with `Document(output)` and check requested text, tables,
styles, images, and section settings. For edits, compare the affected content and
unrelated parts against the original. A successful reopen does not validate every
OOXML feature or prove pagination. Follow the verification reference for rendered
review when an appropriate viewer or conversion tool is available; otherwise state
the coverage limit. There is no `BuildDocx` or `InspectDocx` contract in this skill.

Confirm the nonempty output using `ListDirectory` or `GlobFiles`. In a collaboration
room, return a verified link such as `[Report](room://Report.docx)`; encode special
characters in each path segment, preserving `/`. It opens the existing Word panel
with its Save and Download controls. Outside a room use the actual platform artifact
link. Do not claim you inspected the browser rendering merely because the file can
be opened there. Comments and tracked-change controls are currently hidden in the
Word panel, so use a capable Word client to review those features.
