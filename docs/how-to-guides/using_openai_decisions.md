# OpenAI Decisions

Use `OpenAIDecisions(engine, input, questions, paramValues)` to call OpenAI's
Decisions API through a registered OpenAI model engine. This operation uses
OpenAI's native question and answer formats. It is separate from `TypeSafe(...)`
and is not available in the TypeSafe decision router.

## Configure a model

Use a Python-backed `OpenAiEngine` configured with a model that supports the
Decisions API. For example:

```properties
ENGINE <openai-engine-id>
ENGINE_ALIAS OpenAI Decisions
ENGINE_TYPE prerna.engine.impl.model.OpenAiEngine
MODEL_TYPE OPEN_AI
MODEL gpt-6-luna
INIT_MODEL_ENGINE from genai_client import OpenAiClient;${VAR_NAME}=OpenAiClient(api_key=__import__('os').environ['OPENAI_API_KEY'], model_name='${MODEL}')
```

Set `OPENAI_API_KEY` in the environment inherited by the engine's Python process.
Existing OpenAI engines can be used if their configured model and endpoint
support Decisions. For a compatible server, pass its API root as the client's
`endpoint` argument, such as `endpoint='http://localhost:8080/v1'`.

The engine's Python environment needs the updated `genai_client` and OpenAI
Python SDK 3.26.0 or newer; the project's pinned SDK version satisfies this
requirement. Rebuild/restart SEMOSS to load the Java changes and discover the
reactor. The Azure engine does not expose this operation.

## Call from Pixel

```text
OpenAIDecisions(
  engine=["<openai-engine-id>"],
  input=["I was charged twice and need a refund today."],
  questions=[{
    "name": "department",
    "type": "choice",
    "instructions": "Which team should handle this ticket?",
    "choices": [
      {"value": "billing", "description": "Payments and refunds"},
      {"value": "technical", "description": "Bugs and errors"}
    ]
  }, {
    "name": "frustration",
    "type": "score",
    "instructions": "How frustrated is the customer?",
    "levels": [
      {"label": "calm", "description": "Calm"},
      {"label": "frustrated", "description": "Frustrated but civil"},
      {"label": "angry", "description": "Very angry"}
    ]
  }, {
    "name": "is_urgent",
    "type": "predicate",
    "instructions": "Does this require action today?"
  }],
  paramValues=[{"timeout": 30, "max_retries": 2}]
);
```

Replace the engine ID with the registered OpenAI engine's UUID. The engine owns
model selection and credentials. `paramValues` cannot override `model`, `input`,
or `questions`, including through `extra_body`.
`paramValues` is optional and accepts supported native Decisions SDK options.
`timeout` is in seconds, and the nonnegative integer `max_retries` controls
retries for this call.
Chat history, templates, and generation defaults are not applied.

Named Pixel arguments use an outer list of values. `input=["text"]` supplies one
string. To use native input items, supply each message as an object in that list:

```text
input=[{
  "role": "user",
  "content": [{"type": "input_text", "text": "Check this ticket."}]
}]
```

`questions` is a list of native question objects, including when there is only
one question. Each question can have a `name`; it is not a map keyed by question
name. Use `predicate`, `choice` with `choices`, or `score` with ordered `levels`.
These are OpenAI schemas, so Jev's `state`, `criteria`, and `noul` fields are not
translated. For structured application data, serialize it into the input text.

## Read the result

The result uses SEMOSS's model-response envelope. For example (illustrative
values):

```json
{
  "response": {
    "model": "gpt-6-luna",
    "answers": [
      {
        "type": "choice",
        "name": "department",
        "choice": "billing",
        "confidence": 0.9,
        "probabilities": [
          {"value": "billing", "probability": 0.9},
          {"value": "technical", "probability": 0.1}
        ]
      },
      {
        "type": "score",
        "name": "frustration",
        "score": 0.75,
        "confidence": 0.6,
        "probabilities": [
          {"label": "calm", "value": 0, "probability": 0.25},
          {"label": "frustrated", "value": 1, "probability": 0.75},
          {"label": "angry", "value": 2, "probability": 0.0}
        ]
      },
      {"type": "predicate", "name": "is_urgent", "probability": 0.99}
    ],
    "usage": {"input_tokens": 425, "output_tokens": 0, "total_tokens": 425}
  },
  "numberOfTokensInPrompt": 425,
  "numberOfTokensInResponse": 0
}
```

Read the array at `result.response.answers`, in question order, or find an answer
by its `name`. An answer can instead have `type: "refusal"`; check its type before
reading `choice`, `score`, or `probability`. Refusals remain explicit answers.
Predicate probabilities are not converted to booleans, and fractional scores
are not rounded. Score values correspond to zero-based level positions.

The native result, including provider metadata and usage, is preserved inside
`response`. An applicable `usageRestriction` is also included in the SEMOSS
envelope. The reactor checks engine access, and the engine applies platform
usage limits and inference logging. The input must fit the remaining token and
cost budget before the provider is called. Cost accounting uses the engine's
configured pricing metadata; configure Decisions rates for accurate cost limits.
This operation does not create a chat conversation.

The platform entry point is the Pixel reactor. The OpenAI-compatible REST and
Python passthrough endpoints do not yet expose `/decisions`.

For direct Python calls, see the
[OpenAI client documentation](../python_genai_client/text_generation/openai.md#decisions-api).
For native schemas and supported models, see the
[OpenAI Decisions guide](https://developers.openai.com/api/docs/guides/decisions).
