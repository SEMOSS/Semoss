---
name: model
description: Use when writing code in an app that calls an LLM, embedding model, or other model engine, OR when listing/selecting models the user has access to. Covers the LLM() and MyEngines() pixel commands via @semoss/sdk's runPixel, including prompt/completion calls, conversational history, image inputs, and parsing model responses. Do not use for vector database queries (see semoss-vector) or guardrail engines (see semoss-guardrail).
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

The variations below show only the pixel string — the one that goes inside the `runPixel` template literal. The surrounding `runPixel(...)` call, the `errors` check, and the response parsing are the same as above.

### Conversational history

Pass a `full_prompt` array inside `paramValues`. When using `full_prompt`, the `command` field is ignored — pass `"ignore"` as a placeholder.

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

### Structured Outputs

Pass 'schema' in the `paramValues` to have the model return structured json.

```
LLM ( engine = "${MODEL_ID}" , command = "Sample Question" , paramValues = [ { 'schema' : { "type" : "object" , "properties" : { "sample_property" : { "type" : "array" , "items" : { "type" : "object" , "properties" : { "sample_property_1" : { "type" : "string" } , "sample_property_2" : { "type" : "string" } } , "required" : [ "sample_property_1" , "sample_property_2" ] } } } , "required" : [ "sample_property" ] } } ] ) ;
```

### Images

Pass either a public `url` or a server-accessible `image` filename as a top-level argument. Use `roomId` to thread multiple image turns into the same conversation.

```
LLM(engine="${MODEL_ID}", roomId="my_room_id", command=["What is in this image?"], url="https://example.com/image.png");
```

```
LLM(engine="${MODEL_ID}", roomId="my_room_id", command=["What is in this image?"], image="myImage.png");
```

## Response shape

`pixelReturn[0].output` contains:

- `response` — the model's text output (primary field for simple completions)
- `parts[]` — structured parts with `{text, type}`, used for multi-part responses
- `messageType` — `"CHAT"` for conversational models
- `numberOfTokensInResponse`, `numberOfTokensInPrompt` — token accounting
- `messageId`, `roomId` — for tracking turns in a conversation

For the full response schema, see `references/response-schema.md`.

## Listing available models

Before calling a model, you often need to let the user pick one — or find one programmatically. Use the `MyEngines` pixel with `engineTypes=["MODEL"]` to list models the current user has access to.

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

### Filtering and paging

`MyEngines` accepts several optional arguments. All are arrays, even when passing a single value:

- `filterWord=["claude"]` — substring match against engine name.
- `limit=[50]`, `offset=[0]` — paging. Omit both to return all results.
- `onlyFavorites=[true]` — restrict to the user's favorited engines.
- `sort={"ENGINENAME": "ASC"}` — sort by `ENGINENAME` or `DATECREATED`, direction `ASC` or `DESC`.

```
MyEngines(engineTypes=["MODEL"], filterWord=["claude"], sort={"ENGINENAME": "ASC"}, limit=[20], offset=[0]);
```

### Response field conventions

Use `engine_*` fields (`engine_id`, `engine_name`, `engine_display_name`, `engine_subtype`, etc.). The response also contains `app_*` and `database_*` fields with the same values — these are legacy aliases and should not be used in new code.

Common pattern — render a picker and use the selected `engine_id` as `MODEL_ID` in the `LLM()` call above:

```typescript
const [models, setModels] = useState<Model[]>([]);
const [selectedId, setSelectedId] = useState<string>("");

useEffect(() => {
  runPixel(`MyEngines(engineTypes=["MODEL"], limit=[50], offset=[0]);`).then(
    ({ pixelReturn }) => setModels(pixelReturn[0].output),
  );
}, []);
```

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

- `insightID` — The insight ID used for the pixel execution.
- `pixelReturn[]` — array of results, one per pixel command in the call. For a single `LLM()` call, always index `[0]`.

## pixelReturn[0] fields

- `pixelId` — sequence ID of the command within the call.
- `pixelExpression` — the parsed pixel string SEMOSS actually executed. Useful for debugging encoding issues.
- `isMeta` — internal flag; ignore for model responses.
- `timeToRun` — execution time in milliseconds.
- `operationType` — categorization of the pixel; `["OPERATION"]` for LLM calls.

## pixelReturn[0].output fields — the model response

- `response` _(string)_ — the model's text output. **Use this for simple text completions.**
- `parts[]` _(array)_ — structured response parts, each `{ text, type }`. Use this instead of `response` when the output contains multiple content types (e.g. text + tool calls). `type` values include `"TEXT"`; other types appear for multimodal or agentic outputs.
- `messageType` — `"CHAT"` for conversational models.
- `io` — `"OUTPUT"` for model responses; `"INPUT"` on echoed prompts.
- `schemaVersion` — response schema version. Currently `2`.
- `numberOfTokensInPrompt`, `numberOfTokensInResponse` — token counts for billing and context tracking.
- `messageId` — unique ID for this specific message. Use to reference a turn in a conversation.
- `roomId` — conversation/session ID. Matches `insightID` for single-turn calls; persists across turns in multi-turn chat.

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
