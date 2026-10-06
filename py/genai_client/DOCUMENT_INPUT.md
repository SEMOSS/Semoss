# Document attachments through text extraction

**Configure what the model accepts. SEMOSS extracts document text when needed.**
A user attaches a document as usual. The platform reads the selected model's
`inputModalities` metadata and either sends the document through the existing
provider path or converts it to text first. **No document-extraction argument is
required in the Python init script.**

| Model input modalities | PDF attachment | Word, PowerPoint, or Excel attachment |
| --- | --- | --- |
| `TEXT` | Extract text | Extract text |
| `TEXT`, `IMAGE` | Extract text | Extract text |
| `TEXT`, `PDF` | Send PDF directly | Extract text |
| `TEXT`, `FILE` | Send directly | Send directly |

`FILE` is intentionally treated as broad native support for all document types,
including PDF. It does not enable image, audio, or video inputs. `PDF` means PDF
support only. For example, a model with `TEXT` and `PDF` still needs PowerPoint
text extraction. The generic catalog `attachment` flag does not imply `FILE`
for this decision; the input modalities select the delivery path.

The provider remains responsible for accepting documents sent directly. If a
model is marked `FILE` but its endpoint rejects a particular format, correct the
capability metadata to enable extraction. This version does not retry a rejected
native request through Docling.

## Configuration

Set the model's input modalities in its existing model metadata (`MODELMETADATA`).
For a text-only model such as Laguna on vLLM, the relevant metadata is:

```json
{
  "inputModalities": ["TEXT"]
}
```

Keep the usual client initializer, with no extra document argument:

```python
client = genai_client.OpenAiClient(
    model_name="laguna",
    api_key="your-key",
    endpoint="http://your-vllm-server:8000/v1",
    chat_type="chat-completion",
)
```

When editing an existing model, keep its existing client variable and values.
The existing `INPUT_MODALITIES` SMSS override is also honored; for example,
`INPUT_MODALITIES TEXT` overrides the saved metadata with text-only input.
Normally the saved model metadata is sufficient.

Deploy the updated Java backend and Python code together, then reload the model
engine. Java reads the metadata on engine open and passes it to Python on each
request. Reload the engine after changing its metadata so the new capabilities
take effect. The current model's capabilities apply to attachments in previous
turns as well as newly uploaded ones.

Models are expected to have input metadata. If it is absent (or the SMSS override
is explicitly blank), automatic extraction is not inferred: existing delivery
is retained. Models without `TEXT` input cannot use the text-extraction fallback.

### Migrating from the earlier init setting

Before removing the old argument, check the model's **saved** input modalities.
They must describe what the model endpoint can read directly. If `FILE` or `PDF`
was previously added just to let users attach documents, remove those entries
for a text-only endpoint. Keep `TEXT`; that now allows supported documents to be
attached and extracted automatically.

Remove `native_document_mime_types=...` from `INIT_MODEL_ENGINE` and remove its
`NATIVE_DOCUMENT_MIME_TYPES` SMSS property if you added one. The input modalities
now make that choice automatically. The old Python argument is still accepted
for compatibility with direct Python callers or older backends that do not send
metadata; **engine input metadata takes precedence whenever it is supplied**.
The old argument is not needed for normal platform configuration.

### Troubleshooting a provider error about a `file` content part

An error such as `Unsupported chat content part type: 'file'` means the provider
received a document through the native path. Check these in order:

1. Read the selected engine's saved `inputModalities`. `FILE` bypasses extraction
   for every document; `PDF` bypasses it for PDFs. A text-only vLLM deployment
   should have `TEXT`, without `FILE` or `PDF`.
2. Reload the model engine after correcting its metadata. An already-open engine
   caches its capabilities, so changing the saved row alone does not update the
   running instance. An owner/admin can run
   `CloseEngine(engine=["YOUR_ENGINE_ID"]);` in Pixel; the next model request
   reopens it with the saved metadata. Restarting the backend also clears it.
3. Confirm that both the Java metadata bridge and the Python extraction code are
   deployed. Updating Python alone does not make an older backend send metadata.

For example, switching from the old forced-extraction `[]` setting to metadata
containing `TEXT, FILE, PDF` changes delivery to native documents. Correct the
metadata to `TEXT` and reload the engine to restore automatic text extraction.

## Supported file types

These are the formats supported when an attachment needs text extraction.
Native document delivery follows the model's input capabilities in the table above.

| Document type | Extensions | Processing |
| --- | --- | --- |
| PDF | `.pdf` | Docling text and table extraction, with local OCR and page labels |
| Word | `.docx` | Docling text and table extraction |
| PowerPoint | `.pptx` | Docling text and table extraction, including slide labels and speaker notes |
| Excel | `.xlsx` | Docling sheet labels and tables |
| Delimited text | `.csv`, `.tsv` | Decode the file's text, retaining delimiters and rows |
| Plain text and logs | `.txt`, `.text`, `.log` | Decode text directly |
| Markdown and reStructuredText | `.md`, `.markdown`, `.rst` | Decode text directly |
| HTML and XML | `.html`, `.htm`, `.xml` | Decode source text, including markup |
| JSON and line-delimited JSON | `.json`, `.jsonl`, `.ndjson` | Decode text directly |
| Configuration files | `.yaml`, `.yml`, `.toml`, `.ini`, `.cfg`, `.conf` | Decode text directly |
| Source code and SQL | `.py`, `.js`, `.ts`, `.tsx`, `.jsx`, `.java`, `.c`, `.h`, `.cpp`, `.hpp`, `.cs`, `.go`, `.rs`, `.rb`, `.php`, `.kt`, `.swift`, `.r`, `.lua`, `.sql` | Decode source text directly |
| Shell scripts | `.sh`, `.bash`, `.zsh` | Decode source text directly |

Text decoding accepts UTF-8 and BOM-marked UTF-16/32. Detection uses the supplied
MIME type, falling back to the filename or file-format metadata when that type is
missing or generic. Other recognized text MIME types are also accepted, including
JSON5, JSON-LD, GraphQL, and `text/*` except RTF/rich text. Unknown binary types
are rejected; a filename alone does not override an explicit binary MIME type.

Old Office formats (`.doc`, `.ppt`, `.xls`), RTF, OpenDocument files, and archives
are not supported by this extraction path. Save Office documents as `.docx`,
`.pptx`, or `.xlsx`, or provide a PDF or supported text file instead. Standalone
image, audio, and video handling is unchanged; enabling document extraction does
not make a text-only model able to understand those attachments. Embedded images
and charts are not visually interpreted.

## Processing

The Python-backed OpenAI, Azure OpenAI, Anthropic, Bedrock, Vertex/Google GenAI,
and text-generation engines pass input modalities to the shared text client.
Java lets documents reach this preparation step; unsupported formats or failed
conversions still fail before the model call. Other media retain their existing
modality validation. Legacy/custom engines outside these integrations keep their
existing behavior.

The shared text client calls `DocumentInputProcessor` after constructing SEMOSS
messages and before the provider-specific builder. Only outgoing copies of input
attachments are transformed, in both current and legacy message schemas. The
stored original attachments remain available to download, preview, or send to a
different model. Earlier attachments are prepared again for the currently
selected engine. The internal capability parameter is consumed before provider
API parameters are built and is not written into stored conversation history.
Batch requests containing SEMOSS `message_json` histories use the same path;
already assembled provider-native batch bodies are sent as supplied.

CSV, TSV, Markdown, JSON, source files, and other recognized text formats are
decoded directly (UTF-8 or BOM-marked UTF-16/32). PDF, DOCX, PPTX, and XLSX use
Docling, imported lazily. PDFs retain page labels, presentations retain slide
labels and speaker notes, and spreadsheets retain sheet labels and tables.
Docling's PDF pipeline includes local OCR. Image descriptions are not enabled;
the extracted text carries a notice for images that were not visually analyzed.

Conversion results are cached in the model process, partitioned by the first
input message's ID and keyed by document bytes and format. The cache holds at
most 32 results / 8 MiB of extracted text. Requests without a conversation ID do
not share cached results. Restarting the model process or changing its code
clears the cache; there are no persistent extracted files to migrate or clean up.

Limits are 20 MiB per file, 200 pages per document, 1,000,000 extracted characters
per request, and a Docling pipeline timeout of 120 seconds. Docling's timeout is
cooperative and does not bound first-time imports/model downloads. Unreadable,
empty, unsupported, oversized, or partially converted files fail before the
provider call. Text is never silently truncated.

When `context_window` is configured, a deliberately conservative preflight counts
UTF-8 text bytes (including conversation, system text and tool definitions) plus
the requested output allowance. This can reject documents that the model's
tokenizer would fit. It avoids claiming an exact token count for unknown models;
the serving endpoint remains authoritative, including native-media token costs.
Large-document retrieval and automatic summarization are not performed.

## Runtime and Playground

Install Docling in the Python environment used by the **model engine**. PDF
conversion may download Docling/OCR model artifacts on first use. Pre-download
them for an offline deployment. Conversion does not enable remote document
processing services. Document URLs are not fetched; upload the file instead.
Old binary Office files (`.doc`, `.ppt`, `.xls`) must first be saved as their
modern equivalents. Image/audio/video attachment handling is unchanged.

Playground's configured `allowedFileTypes` remains a separate platform policy.
Include document extensions there if an allow-list is in use. An empty list
allows all types at the UI policy layer, with extraction support checked on the
backend. Rejected attachments now fail the submission instead of disappearing
from the provider request. The existing loading indicator remains active during
conversion; this version does not add per-file progress events or a new picker.

## Validation

```sh
PYTHONPATH=py py/install_config/.venv/bin/python -m pytest \
  py/genai_client/tests/test_document_input.py \
  py/genai_client/tests/test_attachment_mime_types.py -q

SEMOSS_TEST_DOCLING=1 PYTHONPATH=py py/install_config/.venv/bin/python -m pytest \
  py/genai_client/tests/test_document_input_docling.py -q

mvn -Dtest=AbstractPythonModelEngineDocumentInputUnitTests,AbstractPythonModelEngineTokenLimitsUnitTests,AbstractModelEngineInputModalityUnitTests test
```

The second command generates synthetic files and runs real local conversions;
it can initialize/download PDF model artifacts. No user documents or remote
language-model calls are needed by these tests.
