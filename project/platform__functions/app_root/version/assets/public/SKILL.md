---
name: functions
description: Use when writing code in an app that calls a function engine or guardrail engine on the platform - OCR / document extraction (Textract, Google OCR, Azure Document Intelligence), audio transcription (AWS/OpenAI Transcribe), image description, admin-registered REST APIs, registered Python functions, or screening user input with a guardrail before an LLM call. Covers ExecuteFunctionEngine(), GetFunctionEngineDefinition(), ExecuteStreamingFunctionEngine(), ExecuteGuardrailEngine(), GetGuardrailEngineDefinition(), and GetEngineUsage() via @semoss/sdk's runPixel, plus listing engines with MyEngines(engineTypes=["FUNCTION"]) or ["GUARDRAIL"]. Do not use for direct LLM calls (see model) or vector search (see vector).
---

# Function and guardrail engines

A **function engine** is a server-side tool registered on the platform: OCR and document extraction, speech-to-text, image description, an enterprise REST API an admin has wrapped, or a registered Python function. When a model performs tool use, the tools it invokes are function engines - so this is also how an app calls one of those tools directly, without going through a model.

A **guardrail engine** is the screening variant: it takes text and returns a pass/fail verdict (prompt-injection detection, toxicity, PII/NER labeling, on-topic checks, policy compliance).

All calls go through `runPixel` from `@semoss/sdk`.

## Discover, then execute

Function engines take **named** parameters that differ per engine. Never guess them - fetch the contract first:

```typescript
import { runPixel } from "@semoss/sdk";

const FUNCTION_ID = "0af7cfc1-6da1-4c47-a2a0-b03b95b03712"; // the project's selected function engine

// 1. what parameters does this engine take?
const def = await runPixel(
  `GetFunctionEngineDefinition(engine="${FUNCTION_ID}");`,
  insightId,
);
const contract = def.pixelReturn[0].output as {
  name: string;
  description: string;
  parameters: { type: "object"; properties: Record<string, { type: string; description: string }> };
  required: string[];
};

// 2. execute with a map of those named parameters
const result = await runPixel(
  `ExecuteFunctionEngine(engine="${FUNCTION_ID}", map=[${JSON.stringify({
    file_path: uploaded.fileName, // whatever the contract declares
  })}]);`,
  insightId,
);
if (result.errors.length) throw new Error(result.errors[0]);
const output = result.pixelReturn[0].output; // raw engine output - shape is engine-specific
```

Points that matter:

- `map` is a JSON object of the engine's **named** parameters (not positional). The output shape is whatever the engine produces - inspect one response before binding UI to it.
- The user needs view access to the engine; otherwise the pixel errors with "does not exist or user does not have access".
- For file-taking engines (OCR, transcription), upload the file to the insight space first (see the file-uploads skill) and pass the resulting file name/path in `map`.
- `ExecuteStreamingFunctionEngine(engine, map)` exists for streaming REST engines only; it errors with "This engine is not a streaming function engine" otherwise. Run it via `runPixelAsync` and poll `getPixelJobStreaming` (see app-bootstrap's Streaming section) to render partial output.

## Guardrails - screen input before the LLM sees it

```typescript
const GUARDRAIL_ID = "b2d1a7e3-5f60-4d2b-9c4e-8a1f2e3d4c5b";

const check = await runPixel(
  `ExecuteGuardrailEngine(engine="${GUARDRAIL_ID}", prompt="<encode>${userText}</encode>", threshold=0.8);`,
  insightId,
);
const verdict = check.pixelReturn[0].output as {
  pass: boolean;
  returnPrompt: string;          // the text to forward (may be masked/cleaned)
  fullDetails: Record<string, unknown>;
};

if (!verdict.pass) {
  showBlockedMessage();
} else {
  await askModel(verdict.returnPrompt); // forward returnPrompt, not the raw input
}
```

Unlike `ExecuteFunctionEngine`, guardrail parameters are passed as **sibling pixel arguments**, not inside a `map`. The parameter set depends on the guardrail type - call `GetGuardrailEngineDefinition(engine=...)` to fetch it. Common shapes:

| Guardrail type | Parameters |
| --- | --- |
| Prompt injection | `prompt`, `threshold`, `maxLength` |
| Toxicity (Detoxify) | `prompt`, `threshold` |
| Entity/PII labeling (GLiNER) | `prompt`, `labels`, `threshold` |
| On-topic | `prompt`, `threshold`, `limit` |
| Policy compliance | `prompt`, `policy` |
| Self-harm | `prompt` |

Always forward `returnPrompt` rather than the original input - masking guardrails rewrite the text (e.g. PII redaction) even when they pass.

## Listing available engines

```typescript
const { pixelReturn } = await runPixel(
  `MyEngines(engineTypes=["FUNCTION"], limit=[50], offset=[0]);`,
  insightId,
);
// engine_subtype distinguishes OCR vs transcription vs REST etc.
```

Same for `["GUARDRAIL"]`. `MyEngines` supports `filterWord`, `sort`, `limit`, `offset` as described in the database skill.

Never hardcode or guess an engine ID. Use the project's selected engines (see the selected-engines skill); if none fits, ask the user to choose or attach one.

## Self-documenting fallback: GetEngineUsage

For any engine - including ones added after this skill was written - the platform can generate its own current usage docs:

```typescript
const usage = await runPixel(`GetEngineUsage(engine="${FUNCTION_ID}");`, insightId);
// -> list of tabbed docs (introduction, pixel, javascript, python) with
//    copy-pasteable snippets, and for FUNCTION engines the live parameter
//    contract plus a pre-filled map=[...] argument
```

`GetEngineUsage(type="FUNCTION")` gives the generic docs for an engine type. When the contract from `GetFunctionEngineDefinition` is ambiguous, this is the authoritative source for correct call syntax.
