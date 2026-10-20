# Find and read room files

## Choose the right source

Distinguish a file already in the room from an email attachment listed in the thread,
a cloud file, and a file created by an earlier turn. A filename in an email is metadata;
it does not establish that the file has been downloaded or read.

- First use a path returned by an upload or successful tool result, or an exact path
  supplied by the owner. Preserve spaces and extensions; do not replace it with a
  guessed absolute filesystem path.
- If the location is unknown, use `ListDirectory` on a likely folder or `GlobFiles`
  with a narrow pattern. Inspect the result before choosing a file.
- Use `GrepFiles` for text content, with a relevant path or file glob. Do not search
  every skill folder or unrelated output when a small set of task files is sufficient.
- If two files could be the requested source, compare their metadata or opening text.
  Ask which one only if the distinction materially affects the answer.

Example tool arguments, using the actual schema exposed in this run:

```json
{"pattern":"**/*.xlsx"}
```

```json
{"path":"notes","pattern":"decision","glob":"*.md","output_mode":"content","head_limit":40}
```

## Read with coverage in mind

Use `ReadFile` for text. It returns line numbers and reports when more content remains.
Read the relevant continuation or sections before asserting whole-file coverage.
Do not infer that the end of one chunk is the end of the document.

For PDFs, Office documents, images, and other binary formats, load
[documents/read-and-extract.md](../documents/read-and-extract.md). A binary file
does not become readable text by changing its extension or opening it with `ReadFile`.
For document text, load `python` and use `from smssutil import get_document_markdown`
with the exact listed file path. It reads under ROOT and returns Markdown without
modifying the file. Keep native readers for exact formulas or format-specific details.

Track the exact source filename and the sections, pages, sheets, or slides used.
When summarizing multiple revisions, inspect their contents and establish which
revision the owner wants. A filename suffix alone is insufficient evidence of authority.

## Recover from missing or unreadable files

On a missing-path error, list the expected folder once and use a returned path.
On an access failure, stop and describe the missing access. On a parser failure,
try an available format-appropriate reader or request an accessible version. Do not
repeat an unchanged failing call, invent content, or claim to have reviewed the file.

Inspect sources without changing them. Skill resources and internal runtime folders
are not user documents. Existing generated files are useful only after checking that
they belong to this request and contain the expected content.
