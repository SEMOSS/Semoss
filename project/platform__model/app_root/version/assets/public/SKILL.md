---
name: model
description: Use when writing code in an app that calls an LLM, embedding model, or other model engine, OR when listing/selecting models the user has access to. Covers the LLM() and MyEngines() pixel commands via @semoss/sdk's runPixel, including prompt/completion calls, conversational history, structured outputs, attaching media of any type (image, PDF, Word, spreadsheet, audio, video) with the media argument, and parsing model responses. Also covers which top-level arguments full_prompt drops, media included, plus the other model pixels: Embeddings(), MultiModalEmbeddings(), Vision(), NER(), GetContextWindow(), GetUserModelUsage(), and the provider batch API (BatchLLM, GetModelBatchStatus, GetModelBatchResults). Do not use for vector database queries (see vector).
---

# Model Engine

Call models from the model using `runPixel` from `@semoss/sdk` with the `LLM()` pixel command.

## Usage

```typescript
import { runPixel } from "@semoss/sdk";

const prompt = "Hello";
const MODEL_ID = "6dd0bbfd-cd3b-4f2c-b13a-fe4545872e3d";

const { errors, pixelReturn } = await runPixel(
  `LLM(
    engine="${MODEL_ID}",
    command=["${prompt}"],
    paramValues=[{"temperature":0.1, "max_tokens":2000}]
  );`,
);

if (errors.length) throw new Error(errors[0]);

const response = pixelReturn[0].output.response;
```

The variations below show only the pixel string - the one that goes inside the `runPixel` template literal. The surrounding `runPixel(...)` call, the `errors` check, and the response parsing are the same as above.

## Conversational history

Pass a `full_prompt` array inside `paramValues`. When using `full_prompt`, the `command` field is ignored - pass `"ignore"` as a placeholder.

```
LLM(engine="${MODEL_ID}", command=["ignore"], paramValues=[{
    "full_prompt": [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Who won the world series in 2020?"},
        {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
        {"role": "user", "content": "Where was it played?"}
    ],
    "max_completion_tokens": 2000,
    "temperature": 0.3
}]);
```

A `system` entry sets the system prompt; it is not appended as a message. By default `full_prompt` **replaces** the room's history for this call. Add `"append_full_prompt": true` alongside it to append to the existing history instead.

**full_prompt takes over the whole message payload.** It is not a partial override. The server builds the entire provider payload from the `full_prompt` array alone, so every top-level argument that contributes content is dropped:

| Top-level argument | With `command` | With `full_prompt` |
| --- | --- | --- |
| `command` | the prompt | ignored, pass `"ignore"` |
| `media` | attached | **dropped** |
| `url` | attached | **dropped** |
| `roomId` | selects the room | still honored |
| `paramValues` (`schema`, `temperature`, `max_completion_tokens`, `tools`) | honored | still honored |

So `full_prompt` plus a top-level `media=` silently sends a prompt with no attachment. Nothing errors: the model just answers as if no file were there. Put the media inside `full_prompt` instead, or use the `command=` form.

## Media inside full_prompt

Media is supported inside `full_prompt`, but only in OpenAI content-part form. Make the user entry's `content` an array of parts:

```
LLM(engine="${MODEL_ID}", command=["ignore"], paramValues=[{
    "full_prompt": [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": [
            {"type": "text", "text": "What is in this image?"},
            {"type": "image_url", "image_url": {"url": "https://example.com/image.png"}}
        ]}
    ],
    "temperature": 0.3
}]);
```

Only these part shapes are recognized:

| Part | Chat Completions form | Responses API form |
| --- | --- | --- |
| text | `{"type": "text", "text": "..."}` | `{"type": "input_text", "text": "..."}` |
| media | `{"type": "image_url", "image_url": {"url": "..."}}` | `{"type": "input_image", "image_url": "..."}` |

The media value must be either a public `http(s)` URL or a base64 data URI (`data:image/png;base64,...` or `data:application/pdf;base64,...`), which the server writes into the room folder. A bare uploaded filename does **not** resolve here - that only works with the top-level `media=` argument, which `full_prompt` drops. To send an uploaded file alongside a multi-turn history, either inline it as a data URI or switch to the `command=` form.

Despite the `image_url` part name, this is not images-only either. Any file type the model accepts can travel this way.

**Any other part type is silently skipped.** In particular a `{"type": "MEDIA", ...}` part - the internal wire name used elsewhere in the platform - contributes nothing, and a user entry whose only part is unrecognized becomes an empty message. There is no error to tell you this happened.

## Structured Outputs

Pass 'schema' in the `paramValues` to have the model return structured json.

```
LLM ( engine = "${MODEL_ID}" , command = "Sample Question" , paramValues = [ { 'schema' : { "type" : "object" , "properties" : { "sample_property" : { "type" : "array" , "items" : { "type" : "object" , "properties" : { "sample_property_1" : { "type" : "string" } , "sample_property_2" : { "type" : "string" } } , "required" : [ "sample_property_1" , "sample_property_2" ] } } } , "required" : [ "sample_property" ] } } ] ) ;
```

## Media: files of any type

Attach a file with the top-level `media` argument, or a remotely hosted one with `url`. Use `roomId` to thread multiple media turns into the same conversation.

```
LLM(engine="${MODEL_ID}", roomId="my_room_id", command=["What is in this file?"], media="report.pdf");
LLM(engine="${MODEL_ID}", roomId="my_room_id", command=["What is in this image?"], url="https://example.com/image.png");
```

Both accept an array for multiple files:

```
LLM(engine="${MODEL_ID}", command=["Compare these"], media=["q1.xlsx", "q2.xlsx"]);
```

**`media` is not images-only.** Pass any file type: image, PDF, Word document, spreadsheet, CSV, audio, video. The mime type is derived from the file extension and the file is handed to the model as-is. Whether the model can actually read a given type is a property of the model, not of this pixel - a vision model will read a PNG, a model without document support will not read a DOCX. There is no per-type allow-list here, so check the model's own capabilities when picking one.

`media` takes the filename of a file already uploaded into the insight or room - the `fileName` or `fileLocation` value an upload returns. See the `file-uploads` skill for getting a file there. It also accepts a base64 data URI directly (`data:image/png;base64,...`, `data:application/pdf;base64,...`).

`media` is the only name to use. An older `image` argument is still accepted by the server so existing pixels keep running, but it is a deprecated legacy alias - never write it in new code and replace it with `media` when editing a call that still uses it.

These arguments only work with the `command=` form. Both are dropped when `full_prompt` is used, as described under Conversational history above.

Structured output composes with media: keep `schema` in `paramValues` and pass `media=` or `url=` at the top level in the same call.

```
LLM(engine="${MODEL_ID}", command=["Extract the line items"], media="invoice.pdf",
    paramValues=[{"schema": { ... }, "temperature": 0.1}]);
```

## Response shape

`pixelReturn[0].output` contains:

| Field | Meaning |
| --- | --- |
| `response` | the model's text output, the primary field for simple completions |
| `parts[]` | structured parts with `{text, type}`, used for multi-part responses |
| `messageType` | `"CHAT"` for conversational models |
| `numberOfTokensInResponse`, `numberOfTokensInPrompt` | token accounting |
| `messageId`, `roomId` | for tracking turns in a conversation |

For the full response schema, see `references/response-schema.md`.

## Listing available models

This listing pattern is for app-runtime features where the app's end user picks a model. When *you* are deciding which model the app should call, do not enumerate accessible models: use the project's selected model engine (see the Selected Engines section of your system prompt), and only ask the user to choose or attach one when none is selected.

For the app-runtime case, use the `MyEngines` pixel with `engineTypes=["MODEL"]` to list models the current user has access to.

```typescript
import { runPixel } from "@semoss/sdk";

const { errors, pixelReturn } = await runPixel(
  `MyEngines(engineTypes=["MODEL"], limit=[50], offset=[0]);`,
);

if (errors.length) throw new Error(errors[0]);

const models = pixelReturn[0].output as Array<{
  engine_id: string;
  engine_name: string;
  engine_display_name: string;
  engine_subtype: string; // e.g. "CLAUDE", "OPEN_AI", "VERTEX"
  engine_cost: string;
  engine_favorite: 0 | 1;
}>;
```

## Filtering and paging

`MyEngines` accepts several optional arguments. All are arrays, even when passing a single value:

| Argument | Example | Purpose |
| --- | --- | --- |
| `filterWord` | `filterWord=["claude"]` | substring match against engine name |
| `limit`, `offset` | `limit=[50]`, `offset=[0]` | paging; omit both to return all results |
| `onlyFavorites` | `onlyFavorites=[true]` | restrict to the user's favorited engines |
| `sort` | `sort={"ENGINENAME": "ASC"}` | sort by `ENGINENAME` or `DATECREATED`, direction `ASC` or `DESC` |

```
MyEngines(engineTypes=["MODEL"], filterWord=["claude"], sort={"ENGINENAME": "ASC"}, limit=[20], offset=[0]);
```

## Response field conventions

Use `engine_*` fields (`engine_id`, `engine_name`, `engine_display_name`, `engine_subtype`, and so on). The response also contains `app_*` and `database_*` fields with the same values - these are legacy aliases and should not be used in new code.

Common pattern - render a picker and use the selected `engine_id` as `MODEL_ID` in the `LLM()` call above:

```typescript
const [models, setModels] = useState<Model[]>([]);
const [selectedId, setSelectedId] = useState<string>("");

useEffect(() => {
  runPixel(`MyEngines(engineTypes=["MODEL"], limit=[50], offset=[0]);`).then(
    ({ pixelReturn }) => setModels(pixelReturn[0].output),
  );
}, []);
```

## Beyond LLM() - other model pixels

### Embeddings

Text to vectors, for custom similarity/clustering/dedup work outside a vector database (for document search with a vector DB, use the vector skill instead):

```
Embeddings(engine="${EMBEDDING_MODEL_ID}", values=["first text", "second text"]);
```

Returns a map with the vectors in `pixelReturn[0].output`. If the engine is not an embedding model, the output is the literal string "This model does not support embeddings." - check for it. `MultiModalEmbeddings(engine=, text=, image=, video=)` embeds any combination of modalities (image/video accept base64, data URLs, or remote URLs), with results broken out by modality.

### Vision and NER

```
Vision(engine="${MODEL_ID}", command=["Describe this diagram"], image=["${urlOrBase64}"]);
NER(engine="${NER_MODEL_ID}", prompt=["<encode>${text}</encode>"], entities=["PERSON","EMAIL"], maskEntities=[true]);
```

`Vision` is the image-text-to-text task for models exposing it. `NER` extracts (and with `maskEntities` redacts) named entities - the engine must actually be an NER model or the call fails.

### Token budget and usage

```
GetContextWindow(model="${MODEL_ID}");            // -> context window as an int (0 if unset)
GetUserModelUsage(engine=["${MODEL_ID}"]);        // -> current user's INPUT_TOKENS, OUTPUT_TOKENS, CACHE_* , THINKING_TOKENS, TOTAL_REQUESTS (optional startDate/endDate)
GetUserModelUsageRestrictions(engine="${MODEL_ID}"); // -> the user's quota/restriction state
```

Use `GetContextWindow` before trimming history client-side (or trigger `CompactRoomMessages` - see the room skill). Surface `GetUserModelUsageRestrictions` to warn users before they hit a quota mid-task.

### Batch API - bulk work at lower cost

For large offline workloads (classify 10k rows, summarize a document set), the provider batch APIs are typically ~50% cheaper than interactive calls:

```
BatchLLM(engine="${MODEL_ID}", requests=["<encode>prompt one</encode>", "<encode>prompt two</encode>"]);
```

`requests` entries are plain strings or `{command, context}` maps; the call returns a batch id. Then poll:

```
GetModelBatchStatus(engine="${MODEL_ID}", batchId="${batchId}");
GetModelBatchResults(engine="${MODEL_ID}", batchId="${batchId}");
CancelModelBatch(engine="${MODEL_ID}", batchId="${batchId}");
ListModelBatches(engine="${MODEL_ID}", limit=[20]);
```

Batches are owned per user - polling someone else's batch id fails. Completion takes minutes to hours; persist the batch id (see app-data) rather than holding it in component state.

# LLM response schema

Full response shape returned from a `runPixel` call that wraps an `LLM()` command. The top-level response is an envelope; the model output lives at `pixelReturn[0].output`.

## Example response

```json
{
  "insightID": "019db5ef-0bc6-77ad-83ec-ec413ed19271",
  "pixelReturn": [
    {
      "pixelId": "1",
      "pixelExpression": "LLM ( engine = \"...\" , command = [ \"Hello\" ] ... ) ;",
      "isMeta": false,
      "timeToRun": 5151,
      "output": {
        "numberOfTokensInResponse": 12,
        "numberOfTokensInPrompt": 47,
        "schemaVersion": 2,
        "messageType": "CHAT",
        "response": "Hello! How can I help you today?",
        "io": "OUTPUT",
        "parts": [
          { "text": "Hello! How can I help you today?", "type": "TEXT" }
        ],
        "messageId": "019db5ef-46cf-70af-b356-5771ce903f26",
        "roomId": "019db5ef-0bc6-77ad-83ec-ec413ed19271"
      },
      "operationType": ["OPERATION"]
    }
  ]
}
```

## Envelope fields

| Field | Meaning |
| --- | --- |
| `insightID` | the insight ID used for the pixel execution |
| `pixelReturn[]` | array of results, one per pixel command in the call. For a single `LLM()` call, always index `[0]` |

## pixelReturn[0] fields

| Field | Meaning |
| --- | --- |
| `pixelId` | sequence ID of the command within the call |
| `pixelExpression` | the parsed pixel string the platform actually executed. Useful for debugging encoding issues |
| `isMeta` | internal flag; ignore for model responses |
| `timeToRun` | execution time in milliseconds |
| `operationType` | categorization of the pixel; `["OPERATION"]` for LLM calls |

## pixelReturn[0].output fields

| Field | Type | Meaning |
| --- | --- | --- |
| `response` | string | the model's text output. **Use this for simple text completions.** |
| `parts[]` | array | structured response parts, each `{ text, type }`. Use this instead of `response` when the output contains multiple content types, for example text plus tool calls. `type` values include `"TEXT"`; other types appear for multimodal or agentic outputs |
| `messageType` | string | `"CHAT"` for conversational models |
| `io` | string | `"OUTPUT"` for model responses; `"INPUT"` on echoed prompts |
| `schemaVersion` | number | response schema version. Currently `2` |
| `numberOfTokensInPrompt`, `numberOfTokensInResponse` | number | token counts for billing and context tracking |
| `messageId` | string | unique ID for this specific message. Use to reference a turn in a conversation |
| `roomId` | string | conversation/session ID. Matches `insightID` for single-turn calls; persists across turns in multi-turn chat |

## Common access patterns

```typescript
// Simple text completion
const text = pixelReturn[0].output.response;

// Multi-part response (preferred for agentic flows)
const parts = pixelReturn[0].output.parts;
const textParts = parts.filter((p) => p.type === "TEXT").map((p) => p.text);

// Token accounting
const { numberOfTokensInPrompt, numberOfTokensInResponse } =
  pixelReturn[0].output;

// Conversation threading
const { messageId, roomId } = pixelReturn[0].output;
```
