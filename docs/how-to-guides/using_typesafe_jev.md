# TypeSafe / Jev evaluations

Use `TypeSafe(engine, state, questions, paramValues)` for Jev's System One API.
The path is `TypeSafeReactor` → `TypeSafeEngine` → Python
`TypeSafeClientWrapper.ask()` → the SDK's `system_one()`. It does not create chat
messages, run message builders, or call an LLM reactor.

The Java call uses `ITypeSafeEngine` through the engine proxy, preserving configured
input/output pipelines and engine audit logging. `Utility.getModel()` returns a
proxy, so callers must check this interface rather than cast to `TypeSafeEngine`.

## Configure a model

The model's Python environment needs `typesafe-sdk>=0.7.1` (included in
`py/install_config/pyproject.toml`). The engine uses the same Python process and
`VIRTUAL_ENV_ENGINE` configuration as other Python model engines.

Register the engine with `MODEL_TYPE` set to `TYPESAFE`. For an SMSS configuration:

```properties
ENGINE <engine-id>
ENGINE_ALIAS Jev
ENGINE_TYPE prerna.engine.impl.model.TypeSafeEngine
MODEL_TYPE TYPESAFE
MODEL jev-latest
INIT_MODEL_ENGINE from genai_client import TypeSafeClientWrapper;${VAR_NAME}=TypeSafeClientWrapper(api_key=__import__('os').environ['TYPESAFE_API_KEY'], model='${MODEL}')
```

Set `TYPESAFE_API_KEY` in the environment inherited by the engine's Python process.
This example reads the credential at runtime, so the initialization script does
not contain the secret. The SDK also honors `TYPESAFE_BASE_URL`, or you can supply
`base_url` directly to `TypeSafeClientWrapper`. Use the API root, without `/v1/systemone`.
The constructor also accepts `timeout` (seconds) and `max_retries` (default 2).
Rebuild/restart SEMOSS to load the new Java engine and reactor; reactor discovery
is automatic. Model creation through `CreateModelEngine` uses the same
`MODEL_TYPE`, `MODEL`, and `INIT_MODEL_ENGINE` properties in `modelDetails`.

## Call from Pixel

```text
TypeSafe(
  engine=["<engine-id>"],
  state=[{"ticket": "I was charged twice and need a refund today."}],
  questions=[{
    "department": {
      "type": "choice",
      "instructions": "Which team should handle this ticket?",
      "criteria": {"billing": "Payments and refunds", "technical": "Bugs and errors"}
    },
    "frustration": {
      "type": "score",
      "instructions": "How frustrated is the customer?",
      "criteria": ["Calm", "Frustrated but civil", "Very angry"]
    },
    "is_urgent": {
      "type": "noul",
      "instructions": "Does this require action today?"
    }
  }],
  paramValues=[{"timeout": 30, "max_retries": 2}]
);
```

`state` accepts text or structured data. Named Pixel arguments use an outer list
to supply their values. A single supplied value is passed as-is; multiple state
values form a list. To preserve empty or single-element arrays unambiguously,
put the array inside a state object, such as `state=[{"records": []}]`.
`questions` is a nonempty map keyed by the names you want in the answers.
`paramValues` is optional and supports only `timeout` and `max_retries`.
Model selection and credentials belong to the configured engine.

The result uses SEMOSS's model-response envelope. For example (illustrative values):

```json
{
  "response": {
    "model": "jev-1.13.0",
    "usage": {"input_tokens": 425, "output_tokens": 73},
    "answers": {
      "department": {
        "type": "choice",
        "choice": "billing",
        "confidence": 0.8,
        "probabilities": {"billing": 0.9, "technical": 0.1}
      },
      "frustration": {
        "type": "score",
        "score": 0.75,
        "confidence": 0.6,
        "legend": {"0": "Calm", "1": "Frustrated but civil", "2": "Very angry"},
        "probabilities": {"0": 0.25, "1": 0.75, "2": 0.0}
      },
      "is_urgent": {"type": "noul", "noul": 0.99}
    }
  },
  "numberOfTokensInPrompt": 425,
  "numberOfTokensInResponse": 73
}
```

An applicable `usageRestriction` is also included in the envelope. Engine access
is checked before the model is loaded, and usage limits are checked before Python
is called. When inference logging is enabled, evaluations are recorded with
method `system_one`, input/output token counts, and the existing input/output
retention policy. This uses the insight's ID for inference records, without a
chat `Room` or conversation history. `LLM`, embeddings, and native model batch
operations are unsupported for this engine.

## Python and response types

```python
import os
from genai_client import TypeSafeClientWrapper
from typesafe_sdk import Choice, Noul, Score

with TypeSafeClientWrapper(api_key=os.environ["TYPESAFE_API_KEY"]) as client:
    result = client.ask(
        state={"ticket": "I was charged twice."},
        questions={
            "department": Choice(criteria={"billing": None, "technical": None}),
            "frustration": Score(criteria=["Calm", "Frustrated", "Angry"]),
            "is_urgent": Noul(instructions="Does this require immediate action?"),
        },
        parameters={"max_retries": 0},
    )
    print(result["answers"]["department"]["choice"])
```

Python accepts SDK question objects or the same dictionaries used in Pixel.
`ask()` returns a plain dictionary, rather than an SDK `SystemOneResponse` object.
The wrapper does not convert Noul probabilities to booleans or round Score values.

- **Choice:** `choice`, `confidence`, and probabilities for the named labels.
- **Score:** a potentially fractional `score`, `confidence`, `legend`, and
  probabilities. In SDK 0.7.1, input criteria must be an ordered list; integer-keyed
  criteria maps are from older SDK versions. Output legend/probability keys are
  strings in JSON, even though the SDK's `ScoreAnswer` exposes integer keys.
- **Noul:** `noul`, the probability of yes, between 0 and 1.
- **Metadata:** `model` and `usage` are preserved. Missing/null token counts remain
  unchanged in `response.usage` and count as zero in SEMOSS's numeric accounting.

The SDK validates known answer types. The wrapper then returns the original HTTP
JSON body, retaining extra fields and future answer kinds the SDK would otherwise
discard. HTTP response objects, cached answer groups, and Python class instances
are never sent through the socket. Invalid inputs, provider errors, and invalid
known answer types raise errors; they are not returned as successful evaluations.

Verified against the [official SDK source](https://github.com/typesafe-ai/typesafe-sdk-python),
[question reference](https://docs.typesafe.ai/sdk/python/api/types/questions),
[response reference](https://docs.typesafe.ai/sdk/python/api/types/responses), and
[HTTP API reference](https://docs.typesafe.ai/api).

## Tests

```sh
PYTHONPATH=py python -m pytest -q py/genai_client/tests/test_typesafe_client.py
mvn -Dlicense.skip=true -Dtest=TypeSafeEngineUnitTests,TypeSafeReactorUnitTests,ModelTypeEnumUnitTests test
```

Python tests use the real SDK with an HTTP mock transport. Java tests verify
permissions, typed inputs, safe JSON transport, usage enforcement, and response
accounting. They do not need a live TypeSafe account.
