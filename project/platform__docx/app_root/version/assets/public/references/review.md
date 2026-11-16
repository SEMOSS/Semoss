# Comments and tracked changes

Preserve existing review content. The SEMOSS Word panel currently hides comments
and tracked-change controls. A native review feature can still exist in a delivered
file, but verify it in a capable Word client or inspect its OOXML before describing
it as working. A browser preview without visible markup is not evidence of absence.

## Comments

The standard `python-docx` 1.2 API supports comments anchored to complete runs:

```python
from docx import Document

document = Document(root / "source.docx")
paragraph = document.paragraphs[3]  # Select from inspected content, not this example index.
document.add_comment(
    runs=paragraph.runs,
    text="Confirm this delivery date.",
    author="SEMOSS",  # Use an author supplied by the user when appropriate.
    initials="S",
)
document.save(root / "commented.docx")
```

Select a nonempty sequence of runs. To comment on part of a run, split that run
without dropping its formatting or other XML children first. Verify both the comment
text and its anchor; an unanchored comments part is not a visible review comment.
Threaded replies/resolution and unsupported Word features need explicit OOXML work;
do not promise those from `add_comment` alone.

## Tracked edits

`python-docx` has no high-level revision authoring/acceptance API. Do not call ordinary
text replacement a tracked change. For a scoped redline, edit the inspected package
XML while retaining all unrelated members. WordprocessingML represents inserted
runs in `w:ins` and deleted runs in `w:del`. Deletion text uses `w:delText`. Each
revision has an ID, author, and UTC timestamp. Allocate new IDs after inspecting
existing revisions; preserve their authors and dates.

For a simple, complete text-only run replacement, copy the old run under a deletion
and copy its formatting onto a new run under an insertion. Keep both wrappers at
the old run's paragraph position. Do not nest fresh wrappers blindly into existing
revisions. Partial-run edits require splitting at text boundaries while retaining
run properties; fields, hyperlinks, table structure, and paragraph marks require
their own semantics.

Deletion of text is different from deletion of its paragraph mark. A paragraph can
be empty in the accepted view but still preserve its list item or spacing. Check
both views before claiming that a paragraph has been removed. Avoid ad hoc global
accept/reject transformations on complex documents; they can lose content or leave
incorrect structure. Use a capable review client when the requested acceptance or
rejection cannot be validated here.

Verify rejection of only the new revisions reconstructs the original content, and
acceptance of those revisions produces exactly the requested edit. Confirm changes
outside the requested region and existing revisions remain unchanged. There is no
bundled XSD or complete redline validator; state the actual verification performed.
