# Reasoning & Effort / "Thinking" (`message_builders/semoss_base/reasoning.py`)

The `genai_client` exposes a single, provider-agnostic way to control model
reasoning ("thinking") across every supported provider. Callers send two keys in
the model param map and the client normalizes them into the correct native shape
for whichever provider the engine points to:

* **`thinking`** — turn reasoning on/off. Accepts a bool, the strings
  `"true"`/`"false"`, or an Anthropic/Claude-Code passthrough dict
  (`{"type": "enabled"|"adaptive"|"disabled", "budget_tokens": N}`).
* **`effort`** — how hard to think. One of the canonical levels
  `none | minimal | low | medium | high | xhigh | max`, or an integer token
  budget (which gets bucketed to a level).

The legacy key **`thinking_budget`** (int) is still honored for backwards
compatibility.

> The providers split into two camps. **Effort-based**: OpenAI, modern Anthropic
> (Opus 4.6+/Sonnet 4.6/Fable), Gemini 3.x. **Token-budget-based**: Gemini 2.5,
> Bedrock Converse, legacy Anthropic. A shared canonical `effort ⇄ budget` ladder
> lets one set of inputs work everywhere.

## Where these keys come from

The keys live in the **model param map** that reaches the message builders:

* **`RunAgent(...)` pixel** — pass them under `paramValues`:
  `paramValues=[{"thinking":true, "effort":"high"}]`. (Put them under
  `paramValues`, the model param map — *not* `agentParams`, which is for harness
  hooks.)
* **`LLM(...)` pixel** — same `paramValues=[{...}]` form.
* **Python client (`gaas_gpt_model`)** — `model.ask(..., param_dict={"thinking": True, "effort": "high"})`
  (the wrapper serializes `param_dict` to `paramValues`).
* **SMSS** — `ModelSettings.thinking`, `ModelSettings.effort`, and
  `ModelSettings.thinking_budget` act as fallbacks when the param map omits them.

## Normalization

`normalize_reasoning(param_map, model_settings)` collapses the inputs into a
canonical `ResolvedReasoning(enabled, effort, budget)` (or `None` when reasoning
is off) and **pops** `thinking` / `effort` / `thinking_budget` from the param map
so they never leak into the request body. Each provider's message builder then
maps that result onto its native shape:

| Builder | Method | Native shape |
|---|---|---|
| `openai/openai_message_builder.py` | `_resolve_extended_reasoning` (Responses), `_resolve_reasoning_effort` (Chat Completions) | `reasoning={"effort": ..., "summary": "auto"}` / `reasoning_effort=...` |
| `anthropic/anthropic_message_builder.py` | `_resolve_extended_thinking` | modern: `thinking={"type":"adaptive"}` + `output_config={"effort": ...}`; legacy: `thinking={"type":"enabled","budget_tokens": N}` |
| `google_genai/google_genai_builder.py` | `_resolve_thinking_config` | Gemini 3: `ThinkingConfig(thinking_level=...)`; Gemini 2.5: `ThinkingConfig(thinking_budget=N)` |
| `bedrock/bedrock_message_builder.py` | `_resolve_reasoning_config` / `_apply_reasoning` | `additionalModelRequestFields.reasoning_config={"type":"enabled","budget_tokens": N}` |

## What each canonical `effort` becomes per provider

You always send one of the 7 canonical values; each builder maps it to that
provider's native value (budgets have a floor of `1024`):

| You send `effort` | OpenAI `reasoning.effort` | Claude **modern** `output_config.effort` | Claude **legacy** `budget_tokens` | Gemini **3** `thinking_level` | Gemini **2.5** `thinking_budget` | Bedrock `reasoning_config.budget_tokens` |
|---|---|---|---|---|---|---|
| `none` | `none` | `low` | 1024 | `MINIMAL` | 1024 | 1024 |
| `minimal` | `minimal` | `low` | 1024 | `MINIMAL` | 1024 | 1024 |
| `low` | `low` | `low` | 4096 | `LOW` | 4096 | 4096 |
| `medium` | `medium` | `medium` | 8192 | `MEDIUM` | 8192 | 8192 |
| `high` | `high` | `high` | 24576 | `HIGH` | 24576 | 24576 |
| `xhigh` | `xhigh` | `xhigh` (→ `high` unless Opus 4.7+/Fable) | 32768 | `HIGH` | 32768 | 32768 |
| `max` | `high` | `max` | 63999 | `HIGH` | 63999 | 63999 |

## What each provider *natively* accepts

This is why the mapping clamps — the native domains differ:

| Provider | Native effort domain |
|---|---|
| OpenAI | `none`, `minimal`, `low`, `medium`, `high`, `xhigh` — **but per-model** (gpt-5: minimal/low/medium/high; gpt-5.1: none/low/medium/high; gpt-5.5: +xhigh). No `max`. |
| Claude modern (Opus 4.6+/Sonnet 4.6/Fable) | `low`, `medium`, `high`, `xhigh`, `max` (`xhigh` only Fable/Opus 4.7+) |
| Claude legacy / Bedrock / Gemini 2.5 | token budget (no effort) — effort is bucketed to a budget |
| Gemini 3 | `MINIMAL`, `LOW`, `MEDIUM`, `HIGH` (no xhigh/max) |

## Invalid values — defaults to `medium`

An unrecognized `effort` (typo, `"ultra"`, `"turbo"`, etc.) **never raises**.
`_coerce_effort` returns `None`, then `normalize_reasoning` falls back in order:

1. SMSS `ModelSettings.effort` (if configured)
2. an explicit `thinking_budget` (bucketed to a level)
3. **`DEFAULT_EFFORT = "medium"`** — the final fallback

So `{"thinking": true, "effort": "ultra"}` → **medium**. But
`{"thinking": true, "effort": "ultra", "thinking_budget": 20000}` → **high** (the
budget wins over the unusable string before the default applies).

## Nuances

* **Two clamp layers — only the first defaults to `medium`.** An *invalid* value
  → `medium` (above). A *valid* canonical value the target model doesn't support
  → the builder clamps it to the nearest supported value deterministically (e.g.
  `max`→`high` on OpenAI, `none`→`low` on Claude, `xhigh`→`HIGH` on Gemini 3),
  **not** `medium`.
* **OpenAI is the one place a valid value can still 400.** The client does not
  carry OpenAI's per-model matrix, so `xhigh` / `none` / `minimal` pass through
  as-is. Sending `xhigh` to a model that doesn't accept it (e.g. gpt-5.1) is
  rejected by **OpenAI**, not the client. `low` / `medium` / `high` are safe on
  every OpenAI reasoning model. Claude, Gemini, and Bedrock always self-clamp and
  won't error on effort.
* **`effort: "none"` is not the same as `thinking: false`.** With
  `thinking: true, effort: "none"` you still get a minimal-but-on request (budget
  floored to `1024` / `MINIMAL` / `none`). To fully disable reasoning, send
  `thinking: false` (or omit `thinking`).
* **Budgets are approximate; providers enforce their own ranges.** We do not
  clamp per-model budget ceilings (e.g. Gemini 2.5 Pro caps `thinking_budget` at
  32768), so `max` / `xhigh` on a small-ceiling model may be clamped or rejected
  by that provider.

## Examples

Bare boolean — reasoning on at the default `medium` on every provider:

```
RunAgent(
  roomId=["my-room-123"],
  engine=["<model-engine-id>"],
  command=["<encode>Find the failing test and propose a fix</encode>"],
  paramValues=[{"thinking":true}]
);
```

Explicit effort:

```
paramValues=[{"thinking":true, "effort":"high"}]
```

Legacy token budget (bucketed for effort-based providers, used directly for
budget-based ones):

```
paramValues=[{"thinking":true, "thinking_budget":12000}]
```

Turn reasoning off:

```
paramValues=[{"thinking":false}]
```

Python client:

```python
model.ask(command="Summarize this incident", param_dict={"thinking": True, "effort": "high"})
```
