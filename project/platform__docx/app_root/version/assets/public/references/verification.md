# Word verification and conversion

Reopen the saved file with `python-docx` and inspect the requested content and
formatting. `ZipFile(path).testzip()` checks ZIP integrity, not Word schema validity.
For an edit, compare preserved content and package parts. Successful text extraction
does not prove images, numbering, fields, or pagination are correct.

## Browser inspection

The existing Word panel can render and edit the saved `.docx`. Return the verified
room link to open it. If browser tools are actually exposed, inspect the rendered
pages for clipping, table widths, page breaks, headers/footers, and image placement.
Record which pages were inspected. Reinspect after meaningful layout edits.

Do not assume access to the user's browser or desktop Word. If only native readback
ran, say so. Browser layout may differ from Word's pagination and installed fonts.
The panel's hidden review controls cannot establish tracked-change correctness.

## Existing UnoServer route

SEMOSS already has `ConvertFileUnoserver`, which uses the deployment's configured
LibreOffice service. No local `soffice`, Poppler, or Pandoc installation is required.
In managed Python, for a file confirmed to be in the current room/insight root:

```python
from semoss import Insight

conversion = Insight().run_pixel(
    'ConvertFileUnoserver(filePath=["Report.docx"], format=["pdf"], '
    'outputFilePath=["Report-review.pdf"], space=["insight"]);'
)
conversion
```

Use actual filenames and an unused output path; inspect the returned `pixelReturn`
entries for errors before proceeding. Confirm the PDF with room file tools, then
use an available PDF viewer/image tool to inspect it. Conversion alone is not visual
inspection. The PDF is a review copy; deliver the editable `.docx` too.

`space` and file paths belong to Pixel's asset root, not automatically Python's
current ROOT. A RunAgent target can be a project, user space, or a subdirectory.
Supply the known `space` and path relative to that space, including its subdirectory.
Do not pass an absolute server path or guess a project ID. If the asset scope is
unknown or the converter errors, preserve the document and disclose that conversion
was unavailable. Use the configured service only when that processing is allowed
for the supplied document; do not send it to an arbitrary conversion website.

The same reactor can convert a legacy `.doc` to a new `.docx` with `format=["docx"]`.
Inspect the converted content before editing. Conversion can change layout and
unsupported features; preserve the original. A `.docm` or signed document needs
special handling because a conversion can remove macros or invalidate signatures.
