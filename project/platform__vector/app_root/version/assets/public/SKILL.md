---
name: vector
description: Use when writing code in an app that does semantic search, RAG, or ingests documents into a vector database on the platform — running nearest-neighbor queries, listing/adding/removing documents, feeding retrieved chunks into an LLM, downloading the original source document behind a citation, or tuning chunking at ingest time. Covers VectorDatabaseQuery(), ListDocumentsInVectorDatabase(), CreateEmbeddingsFromDocuments() and its chunking paramValues, CreateEmbeddingsFromVectorCSVFile(), RemoveDocumentFromVectorDatabase(), VectorFileDownload(), ListAllRecordsInVectorDatabase(), and VectorAttachFileToSource() pixel commands via @semoss/sdk's runPixel, plus listing engines with MyEngines(engineTypes=["VECTOR"]). Do not use for raw SQL/graph queries (see database-engine) or direct LLM calls without retrieval (see model-engine).
---

# Vector Engine

Query and manage a vector database on the platform using `runPixel` from `@semoss/sdk` with the `VectorDatabaseQuery()` pixel command as the primary entry point. Results are typically passed into an `LLM()` call to implement RAG — see the [RAG pattern](#rag-pattern) section below.

## Usage

```typescript
import { runPixel } from "@semoss/sdk";

const VECTOR_ID = "1222b449-1bc6-4358-9398-1ed828e4f26a";
const query = "Time sheet";

const { errors, pixelReturn } = await runPixel(
  `VectorDatabaseQuery(engine="${VECTOR_ID}", command="${query}", limit=5, filters=[], metaFilters=[]);`,
);

if (errors.length) throw new Error(errors[0]);

const hits = pixelReturn[0].output as Array<{
  Score: number;
  idx: number;
  Source: string;
  Modality: string;
  Divider: string;
  Part: string;
  Tokens: number;
  Content: string;
  Weighted_RRF_Score: number;
  BM25_Score: number;
}>;
```

Each hit carries both the text chunk (`Content`) and the source document (`Source`), plus hybrid-search scoring fields. Lower `Score` is closer; `Weighted_RRF_Score` and `BM25_Score` are the underlying component scores from the hybrid rank fusion.

The variations below show only the pixel string — the one that goes inside the `runPixel` template literal. The surrounding `runPixel(...)` call, the `errors` check, and the response parsing are the same as above.

### Filtering by source document

`filters` is an array of `Filter()` expressions matched against the chunk's `Source`. Use this to restrict retrieval to a named subset of documents.

```
VectorDatabaseQuery(engine="${VECTOR_ID}", command="${query}", limit=5, filters=[Filter(Source == ["doc1.pdf", "doc2.pdf"])]);
```

### Filtering by metadata

`metaFilters` is an array of `Filter()` expressions matched against arbitrary metadata keys attached to chunks at embed time.

```
VectorDatabaseQuery(engine="${VECTOR_ID}", command="${query}", limit=5, metaFilters=[Filter(department == "finance")]);
```

Both `filters` and `metaFilters` can be combined in the same call.

### Listing documents in the vector index

Returns one entry per unique `Source` currently indexed.

```
ListDocumentsInVectorDatabase(engine="${VECTOR_ID}");
```

Response shape (at `pixelReturn[0].output`):

```typescript
Array<{ fileName: string; fileSize: number; lastModified: string }>;
```

### Adding documents from the current insight

Use when the files were uploaded into the current insight/room workspace. `CreateEmbeddingsFromDocuments` handles the chunking, embedding, and indexing.

```
CreateEmbeddingsFromDocuments(engine="${VECTOR_ID}", filePaths=["fileName1.pdf", "fileName2.pdf"]);
```

### Adding documents from app or user space

When the files live outside the current insight — in a project/app folder or the user's personal space — pass `space`. Use `space="app_id"` where `app_id` is the project/app UUID, or `space="user"` for the user space.

```
CreateEmbeddingsFromDocuments(engine="${VECTOR_ID}", filePaths=["docs/file1.pdf"], space="app_id");
```

### Adding pre-chunked CSV data

Use `CreateEmbeddingsFromVectorCSVFile` when you have already chunked the source material and want to skip the platform's default splitter. Supported inputs: `.csv` files, or `.zip` archives containing CSV files.

The CSV must have exactly these headers, case-sensitive:

```
Source,Modality,Divider,Part,Tokens,Content
doc1.pdf,text,1,0,120,"First chunk of text"
```

Quotes on values are optional unless a value contains commas, newlines, or quotes.

```
CreateEmbeddingsFromVectorCSVFile(engine="${VECTOR_ID}", filePaths=["fileName1.csv", "fileName2.csv"]);
```

Use the same `space` argument as `CreateEmbeddingsFromDocuments` when the CSVs live in app or user space:

```
CreateEmbeddingsFromVectorCSVFile(engine="${VECTOR_ID}", filePaths=["vector_data/chunks.csv"], space="app_id");
```

### Removing documents from the vector index

Pass `fileNames` — the source identifiers as listed by `ListDocumentsInVectorDatabase`, not file paths.

```
RemoveDocumentFromVectorDatabase(engine="${VECTOR_ID}", fileNames=["fileName1.pdf", "fileName2.pdf"]);
```

### Tuning chunking at ingest time

`CreateEmbeddingsFromDocuments` accepts a `paramValues` map controlling how documents are split and indexed:

```
CreateEmbeddingsFromDocuments(engine="${VECTOR_ID}", filePaths=["file1.pdf"], paramValues=[{"chunkingMethod":"recursive", "contentLength":512, "contentOverlap":64}]);
```

| Key | Meaning |
| --- | --- |
| `chunkingMethod` | `token` (default), `recursive`, or `semantic` |
| `contentLength` / `contentOverlap` | chunk size and overlap (in chunk units) |
| `chunkUnit` | what contentLength counts |
| `extractionMethod` | PDF text extraction: `default` or `fitz` |
| `columnsToIndex` / `columnsToRemove` / `columnsToReturn` | CSV ingestion column control |
| `useHybridSearch`, `keywordSearchParam`, `returnThreshold` | retrieval behavior knobs |
| `customDocumentProcessor`, `customDocumentProcessorFunctionID` | route extraction through a function engine (e.g. OCR - see the functions skill) |

Defaults are sensible; reach for these when chunks come back too coarse/fine or scanned PDFs extract poorly (`customDocumentProcessor` + an OCR function engine).

### Download the source document behind a citation

Every RAG app gets asked to "open the actual PDF". `VectorFileDownload` retrieves the original source files back out of the vector database:

```
VectorFileDownload(engine="${VECTOR_ID}", fileNames=["doc1.pdf"]);
```

Returns a **download key** (operationType includes `FILE_DOWNLOAD`) for the standard two-step download flow (see app-bootstrap): open `${Env.MODULE}/api/engine/downloadFile?insightId=...&fileKey=...`. Multiple `fileNames` come back as a single zip. Missing files are reported as a warning noun rather than an error - check for it. If sources were ingested as pre-chunked CSVs, attach the original file first with `VectorAttachFileToSource(engine=, filePath=, source=)` (requires edit access) so there is something to download.

### Inspect the raw chunks

`ListDocumentsInVectorDatabase` lists documents; to see the actual **chunks** (for a debugging UI, chunk-quality review, or "show matched context" views):

```
ListAllRecordsInVectorDatabase(engine="${VECTOR_ID}");
```

Returns every chunk as `{ Source, Modality, Divider, Part, Tokens, Content }`. This is the full index - page it client-side for large databases.

## Response shape

Unlike `SqlQuery` (which returns a tabular `{ data: { values, headers } }` object), vector pixels return their payload directly at `pixelReturn[0].output`:

- `VectorDatabaseQuery` → array of hit objects with `Score`, `Source`, `Content`, etc.
- `ListDocumentsInVectorDatabase` → array of `{ fileName, fileSize, lastModified }`.
- `CreateEmbeddingsFromDocuments` / `CreateEmbeddingsFromVectorCSVFile` → string status (`"Successfully embedded all files"`) plus a structured `additionalOutput[]` with per-file `{ fileName, status, insertedRecords, failedRecords, totalRecords }`.
- `RemoveDocumentFromVectorDatabase` → status payload confirming the removal.

For the full response schema of each pixel, see `references/response-schema.md`.

## RAG pattern

The typical flow is a two-step chain: retrieve chunks with `VectorDatabaseQuery`, then feed them into an `LLM()` call as grounding context. See the `model-engine` skill for the LLM half.

```typescript
import { runPixel } from "@semoss/sdk";

const VECTOR_ID = "1222b449-1bc6-4358-9398-1ed828e4f26a";
const MODEL_ID = "6dd0bbfd-cd3b-4f2c-b13a-fe4545872e3d";
const question = "Time sheet";

const { pixelReturn: retrievalReturn } = await runPixel(
  `VectorDatabaseQuery(engine="${VECTOR_ID}", command="${question}", limit=3);`,
);

const hits = retrievalReturn[0].output as Array<{
  Source: string;
  Content: string;
}>;
const context = hits
  .map((h) => `* Document Name: ${h.Source}, ${h.Content}`)
  .join("\n");

const prompt = `Answer the question using only the context below.\n\nQuestion: ${question}\n\nContext:\n\`\`\`\n${context}\n\`\`\``;

const { pixelReturn: answerReturn } = await runPixel(
  `LLM(engine="${MODEL_ID}", command=["${prompt}"], paramValues=[{"temperature":0.1}]);`,
);

const answer = answerReturn[0].output.response;
```

Keep the prompt explicit about citing `Source` and `Part`/`Divider` so the model's reasoning trace references the retrieved chunks.

## Listing available vector engines

This listing pattern is for app-runtime features where the app's end user picks a vector database. When *you* are deciding which vector engine the app should use, do not enumerate accessible engines: use the project's selected vector engine (see the Selected Engines section of your system prompt), and only ask the user to choose or attach one when none is selected.

For the app-runtime case, use `MyEngines` with `engineTypes=["VECTOR"]`.

```typescript
import { runPixel } from "@semoss/sdk";

const { errors, pixelReturn } = await runPixel(
  `MyEngines(engineTypes=["VECTOR"], limit=[50], offset=[0]);`,
);

if (errors.length) throw new Error(errors[0]);

const vectorEngines = pixelReturn[0].output as Array<{
  engine_id: string;
  engine_name: string;
  engine_display_name: string;
  engine_subtype: string; // e.g. "FAISS", "CHROMA", "WEAVIATE"
  engine_cost: string;
  engine_favorite: 0 | 1;
}>;
```

### Filtering and paging

`MyEngines` accepts several optional arguments. All are arrays, even when passing a single value:

- `filterWord=["policy"]` — substring match against engine name.
- `limit=[50]`, `offset=[0]` — paging. Omit both to return all results.
- `onlyFavorites=[true]` — restrict to the user's favorited engines.
- `sort={"ENGINENAME": "ASC"}` — sort by `ENGINENAME` or `DATECREATED`, direction `ASC` or `DESC`.

```
MyEngines(engineTypes=["VECTOR"], filterWord=["policy"], sort={"ENGINENAME": "ASC"}, limit=[20], offset=[0]);
```

### Response field conventions

Use `engine_*` fields (`engine_id`, `engine_name`, `engine_display_name`, `engine_subtype`, etc.). The response also contains `app_*` and `database_*` fields with the same values — these are legacy aliases and should not be used in new code.

Common pattern — render a picker and use the selected `engine_id` as `VECTOR_ID` in the `VectorDatabaseQuery()` call above:

```typescript
const [engines, setEngines] = useState<VectorEngine[]>([]);
const [selectedId, setSelectedId] = useState<string>("");

useEffect(() => {
  runPixel(`MyEngines(engineTypes=["VECTOR"], limit=[50], offset=[0]);`).then(
    ({ pixelReturn }) => setEngines(pixelReturn[0].output),
  );
}, []);
```

# Vector engine response schemas

Full response shapes returned from `runPixel` calls that wrap the vector pixels: `VectorDatabaseQuery`, `ListDocumentsInVectorDatabase`, `CreateEmbeddingsFromDocuments`, `CreateEmbeddingsFromVectorCSVFile`, and `RemoveDocumentFromVectorDatabase`. Each call has a common envelope and a pixel-specific `output`.

Note: unlike `SqlQuery`, whose output wraps rows in `{ data: { values, headers } }`, vector pixels put the payload directly at `pixelReturn[0].output` — usually a raw array or string.

## Envelope fields

Same envelope as every other pixel call:

- `insightID` — insight ID used for the pixel execution.
- `pixelReturn[]` — array of results, one per pixel command in the call. For a single vector pixel, always index `[0]`.

Each `pixelReturn[i]` carries `pixelId`, `pixelExpression` (the parsed pixel the platform actually executed — useful for debugging encoding), `isMeta`, `timeToRun` (ms), and `operationType`. For successful embedding calls, `operationType` is `["SUCCESS"]`; for queries and listings it is `["OPERATION"]`.

## `VectorDatabaseQuery` output

`pixelReturn[0].output` is an array of hit objects ranked by hybrid similarity.

### Example response

```json
{
  "insightID": "019dbb4c-db55-7ea8-bc46-41c95ec49287",
  "pixelReturn": [
    {
      "pixelId": "3",
      "pixelExpression": "VectorDatabaseQuery ( engine = \"1222b449-1bc6-4358-9398-1ed828e4f26a\" , command = 'Time sheet' , limit = 3 ) ;",
      "isMeta": false,
      "timeToRun": 1027,
      "output": [
        {
          "Score": 0.6172683238983154,
          "idx": 2,
          "Source": "timesheet_guide.pdf",
          "Modality": "text",
          "Divider": "1",
          "Part": "2",
          "Tokens": 132,
          "Content": "advise that you will use the manual timesheet form until the WBS code is made available...",
          "Weighted_RRF_Score": 0.004918032786885246,
          "BM25_Score": 0.05683182179927826
        }
      ],
      "operationType": ["OPERATION"]
    }
  ]
}
```

### Hit fields

- `Score` _(number)_ — combined hybrid-search score. **Lower is closer.**
- `idx` _(number)_ — internal chunk index within the vector store.
- `Source` _(string)_ — source document identifier (e.g. `"timesheet_guide.pdf"`). Pass this back in `filters` to scope future queries.
- `Modality` _(string)_ — chunk modality: `"text"`, etc.
- `Divider` _(string)_ — page / section boundary the chunk came from. Often maps 1:1 to page number for PDFs.
- `Part` _(string)_ — chunk ordinal within the `Divider` (`"0"`, `"1"`, ...).
- `Tokens` _(number)_ — token count of the chunk's `Content`.
- `Content` _(string)_ — the chunk text to feed into an LLM prompt.
- `Weighted_RRF_Score` _(number)_ — reciprocal-rank-fusion component of the hybrid score.
- `BM25_Score` _(number)_ — lexical BM25 component of the hybrid score.

## `ListDocumentsInVectorDatabase` output

`pixelReturn[0].output` is an array with one entry per unique `Source` currently indexed.

```json
{
  "insightID": "019dbb4c-db55-7ea8-bc46-41c95ec49287",
  "pixelReturn": [
    {
      "pixelId": "7",
      "pixelExpression": "ListDocumentsInVectorDatabase ( engine = \"1222b449-1bc6-4358-9398-1ed828e4f26a\" ) ;",
      "isMeta": false,
      "timeToRun": 19,
      "output": [
        {
          "fileName": "timesheet_guide.pdf",
          "fileSize": 198.26953125,
          "lastModified": "2026-03-27 12:22:27"
        }
      ],
      "operationType": ["OPERATION"]
    }
  ]
}
```

- `fileName` _(string)_ — source identifier. Use this as the `Source` in `filters` and as `fileNames` when removing.
- `fileSize` _(number)_ — size in kilobytes.
- `lastModified` _(string)_ — ISO-like timestamp (`"YYYY-MM-DD HH:mm:ss"`).

## `CreateEmbeddingsFromDocuments` / `CreateEmbeddingsFromVectorCSVFile` output

`pixelReturn[0].output` is a status string. Per-file detail lives in `pixelReturn[0].additionalOutput[0].output`.

```json
{
  "insightID": "019dbb4c-db55-7ea8-bc46-41c95ec49287",
  "pixelReturn": [
    {
      "pixelId": "8",
      "pixelExpression": "CreateEmbeddingsFromDocuments ( engine = \"1222b449-1bc6-4358-9398-1ed828e4f26a\" , filePaths = [ \"/130_WEILER1103_OF306.pdf\" ] ) ;",
      "isMeta": false,
      "timeToRun": 3338,
      "output": "Successfully embedded all files",
      "operationType": ["SUCCESS"],
      "additionalOutput": [
        {
          "output": [
            {
              "fileName": "130_WEILER1103_OF306.csv",
              "status": "SUCCESS",
              "insertedRecords": 1,
              "failedRecords": 0,
              "totalRecords": 1
            }
          ],
          "operationType": ["OPERATION"]
        }
      ]
    }
  ]
}
```

- `output` _(string)_ — human-readable summary (`"Successfully embedded all files"` on full success).
- `operationType` — `["SUCCESS"]` for the top-level operation status.
- `additionalOutput[0].output[]` — per-file record: `{ fileName, status, insertedRecords, failedRecords, totalRecords }`. For mixed-result batches, inspect this array to find partial failures. `fileName` here is the normalized CSV name the platform produced, not necessarily the original upload name.

## `RemoveDocumentFromVectorDatabase` output

Same envelope shape as the embedding calls — a status string at `output` plus a per-file breakdown in `additionalOutput`. Check `operationType` for `"SUCCESS"` and inspect `additionalOutput` if you need per-file confirmation.

## Common access patterns

```typescript
// Retrieve and use content chunks
const hits = pixelReturn[0].output as Array<{
  Source: string;
  Content: string;
  Score: number;
}>;
const contextBlock = hits
  .map((h) => `* Document Name: ${h.Source}\n${h.Content}`)
  .join("\n\n");

// Group hits by source document
const bySource = hits.reduce<Record<string, typeof hits>>((acc, h) => {
  (acc[h.Source] ??= []).push(h);
  return acc;
}, {});

// Check embedding success
const success = pixelReturn[0].operationType.includes("SUCCESS");
const perFile = pixelReturn[0].additionalOutput?.[0]?.output ?? [];
const failed = perFile.filter((f) => f.status !== "SUCCESS");
```
