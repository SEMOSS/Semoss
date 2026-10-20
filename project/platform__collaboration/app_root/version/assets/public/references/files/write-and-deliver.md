# Write, edit, and deliver files

## Preserve the intended artifact

Use the owner's requested filename and format. Otherwise choose a clear filename
such as `summary.md` or `analysis.xlsx`. Keep outputs inside the active working
directory and pass relative paths to file tools. Before overwriting an existing
file, read it and establish whether it is the target or a source to preserve.

Use `WriteFile` for a new text file. For an existing text file, read the affected
context and use `EditFile` with a unique exact match, or `MultiEdit` for related
replacements. If a match fails, read the current content before retrying. Do not
discard user edits by regenerating a whole file from an earlier version.

Create binary documents with a suitable installed library or available document tool,
following [documents/create-and-verify.md](../documents/create-and-verify.md).
Renaming text to `.docx`, `.xlsx`, `.pdf`, or `.pptx` does not create that format.

## Execution and persistence

Load the separate `python` skill for managed execution and runtime-path rules.
Use the supported executor and check its result. For Node work, follow the
`ExecuteNodeCode` contract in the tool schema and the separate `pptx` skill when
making a presentation. Do not launch unsupported build commands or install packages.

Python's current directory may differ from the room's `ROOT`. Resolve the destination
for each execution instead of using a bare relative Python filename or changing the
shared process directory:

```python
from pathlib import Path
from smssutil import smss_get_runtime_var

root = smss_get_runtime_var("ROOT")
if not root:
    raise RuntimeError("ROOT is unavailable for this execution")
output = Path(root) / "summary.txt"
output.write_text("Summary\n", encoding="utf-8")
{"filePath": "summary.txt", "size": output.stat().st_size}
```

Pass the absolute `output` path to document-library save and reopen methods too.
Pass `summary.txt`, not the absolute runtime path, to room file tools.

The room's active working directory is the destination for this task. Do not infer
that writing there publishes a project, saves to cloud storage, or attaches the file
to an email. Those outcomes require their own successful tool results.

Keep source files, generated outputs, and temporary extraction files distinguishable.
Do not change `.claude/skills`, `.semoss`, or `.email-attachments` to work around a
failure. Avoid deleting prior outputs unless the owner requested cleanup.

## Verify before delivery

After writing, use room file tools such as `ListDirectory` or `GlobFiles` to confirm
the exact output is in the room. Check that it is nonempty when content is expected.
Read back text through `ReadFile`; reopen binary files with the appropriate reader
using the exact `ROOT` destination. A Python write and read of the same bare relative
filename can both succeed outside the room and do not prove a room deliverable.
Check the requested content, format, filenames, counts, and calculations. For edits,
check both the requested changes and preservation of unrelated content.

For a set of deliverables, compare the expected filenames with the files actually
saved. A successful script or tool call does not prove every requested output exists.

## Hand the result back accurately

Name the verified file and briefly summarize what it contains. Return a Markdown
room-file link, for example `[summary.docx](room://summary.docx)`. It opens the room's
file panel, which provides Download. A bare filename or bare `room://` URI is not a
clickable handoff. Encode spaces and other special characters in each path segment,
keeping folder separators: `[Final report](room://outputs/Final%20report.docx)`.
Use only the verified room-relative path; omit absolute runtime paths, parent-directory
segments, query strings, and fragments. Never create a link for an unverified file.
If a tool returns a user-accessible artifact link, use that exact link. Do not invent
a download endpoint or a local-machine URL. Files outside the room need their actual
platform handoff rather than a `room://` link.

If the owner asked to attach it to an email, load
[email/attachments-and-send.md](../email/attachments-and-send.md) and use `ComposeEmail`
with the verified path. Creating the file and adding it to a draft are separate results.
Disclose a verification limitation without presenting it as a confirmed pass.

## Recover an authorized local output

When generation or delivery fails, inspect the current room and the target file.
Check `ROOT`, the exact path, and the error before retrying; change the failed
assumption instead of repeating the same call. If an output landed outside the room,
preserve its content and place the authorized deliverable under the current `ROOT`.
Read any existing target first and preserve the owner's edits. Repair the intended
output and repeat the room and format checks without asking the owner to authorize
the same local task again. Ask only when a missing decision or unrelated overwrite
blocks repair. This does not authorize retrying a rejected external action or bypassing
an access failure. Finish with the verified artifact link and a concise outcome.
