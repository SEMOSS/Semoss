# Built-in Tools

Built-in tools (also called server tools or provider-hosted tools) are tools
the model provider executes on its own side - web search, web fetch, code
execution, image generation. SEMOSS never runs them; it only tells the
provider which ones to enable on a request. That enablement travels as a
single request param named `built_in_tools` that rides along on python model
ask calls.

This document covers the catalog file, how a selection is stored and reaches
the provider at runtime, and every way to set the tools - including directly
in an engine's SMSS file.

---

## 1. The catalog: `meta/builtin-tools.json`

The catalog is the single source of truth for which tools exist, which
provider offers them, and what their parameters are. It hot-reloads (the
`StaticBuiltinToolsCatalog` cache keys on file mtime + size), so edits apply
without a server restart.

### Top-level shape

Keyed by **serving provider** (who hosts the API). Two node shapes, never
mixed on one node:

- Direct providers (`openai`, `anthropic`) hold tool definitions directly.
- Multi-vendor hosts (`google`, `bedrock`) nest one more level by **model
  provider**: `google.google` = Gemini tools, `google.anthropic` =
  Claude-on-Vertex tools, `bedrock.openai`, `bedrock.anthropic`.

A vendor's model on another host only gets that host's tools - e.g.
Claude on Vertex gets only the basic `web_search_20250305` (no web fetch, no
code execution), and Bedrock serves no Anthropic server tools at all
(`bedrock.anthropic` is deliberately `{}`).

### Tool definition shape

```json
"web_search": {
  "alias": "web_search_20260318",
  "display_name": "Web Search",
  "description": "...",
  "params": [
    {
      "alias": "max_uses",
      "display_name": "Maximum Uses",
      "type": "optional",          // "required" params are always sent
      "input": "number",           // string | number | boolean | list | map
      "options": [],               // enum choices when non-empty
      "default": 5,
      "show_in_ui": true           // false = hidden provider constant
    }
  ],
  "constraints": {                 // optional gates
    "api": "responses",
    "models": ["openai.gpt-5.4"],
    "regions": ["us-east-1"]
  }
}
```

Two design rules that everything downstream relies on:

1. **The `alias` is the provider's tool type string.** It must be versioned
   exactly where the provider versions its tools (`web_search_20260318` for
   Anthropic, plain `web_search` for OpenAI, `google_search` for Gemini).
2. **A tool's params ARE the provider request fields.** There are no name
   tables in code. Anthropic's `name`/`type` ride as hidden required params,
   Gemini's single map param names the `types.Tool` field, OpenAI's params
   are the tool object's own fields. The builders just spread the resolved
   params and set the alias.

### Defaults are live request values

When a tool is enabled, every param's effective value (`value` if the user
set one, else `default`) is sent to the provider **on every ask**. Unset
optionals (`null`, `[]`, `{}`) are pruned; `false` and `0` are kept.

That means a wrong default does not sit dormant: OpenAI validates tool
objects up front, so one invalid default 400s **all** requests on the engine
even if the tool is never used. When you add or change a catalog entry, run
one live ask with the tool enabled - save/load and UI testing cannot catch a
bad default.

### Saved selections are snapshots

The UI stores a copy of the catalog definition at save time (see below).
Catalog fixes do NOT propagate to engines that already saved a selection.
Remediation is two steps: re-save the tool in the UI (refreshes the snapshot
from the catalog) AND reload the engine (the selection is captured once at
engine open).

---

## 2. The UI / metadata path (the normal way)

1. `GetModelBuiltinTools(engine=["<engineId>"])` resolves the engine's
   serving provider (MODELMETADATA.SERVINGPROVIDER, then SMSS `PROVIDER`,
   then SMSS `MODEL_TYPE`) and model provider (MODELMETADATA.MODELPROVIDER,
   then model.json, then the dotted qualifier in the model id), looks up the
   catalog node, and returns `{engineId, modelId, modelProvider,
   servingProvider, tools, selected}`. The import page calls the same
   reactor with explicit `servingProvider`/`modelProvider`/`modelId` before
   an engine exists.
2. The FE saves through `SetEngineMetadata` with the `builtinTools` meta
   key, which lands in the security DB column **MODELMETADATA.BUILTINTOOLS**.
3. The stored shape is a JSON **object keyed by tool name** whose values are
   the catalog definition copied verbatim, with a `value` field added beside
   `default` on any param the user changed:

```json
{
  "web_search": {
    "alias": "web_search_20260318",
    "display_name": "Web Search",
    "params": [
      { "alias": "max_uses", "default": 5, "value": 3, "show_in_ui": true }
    ]
  }
}
```

Storage rules (enforced on write): anything that is not a JSON object is
rejected with an IllegalArgumentException; tool names are normalized to
lower_snake_case; an empty selection is stored as SQL NULL. On read,
non-object column data is treated as unset. There is no legacy list shape -
that support was removed when the catalog feature shipped (2026-08).

---

## 3. Runtime flow

- `AbstractModelEngine.open` captures the MODELMETADATA row's builtinTools
  into the engine instance (so DB edits need an engine reload).
- `AbstractPythonModelEngine.askCall` injects it as the `built_in_tools`
  param on every python ask **unless the caller already supplied one**.
  Python-backed engines only - REST engines forward raw hyperparams into
  request bodies and are deliberately excluded.
- On the python side, all consumption goes through
  `py/genai_client/message_builders/semoss_base/builtin_tools.py`
  (`normalize_built_in_tools`), which collapses the dict into
  `{name, alias, params: {alias: effective-value}}` selections. **Anything
  that is not the dict shape silently normalizes to no tools.**
- Each builder then constructs its provider-native spec:
  - openai: `{**params, "type": alias}` appended to `tools`
    (Responses API tool objects)
  - anthropic: `{**params}` with `type`/`name` defaulted from the
    selection's alias/name
  - google: `types.Tool(**params)` - the param alias names the Tool field,
    and an empty map means "enabled with provider defaults" (google skips
    the empty-value pruning for this reason)
  - bedrock Converse: name-only `systemTool` (no param slot; bedrock-hosted
    OpenAI web search runs through the OpenAI Responses client instead)

### Precedence (highest wins)

1. SMSS `global_param_override` - merged over the ask kwargs in the python
   client, every ask
2. A `built_in_tools` param supplied on the ask call itself
3. The saved MODELMETADATA selection (the UI path)

Note the sharp edge: a malformed SMSS value does not "fall back" to the UI
selection - it replaces a valid dict with something that normalizes to
nothing, actively disabling tools the UI enabled.

---

## 4. Setting built-in tools in an SMSS

The SMSS init script's `global_param_override` kwarg is merged over the ask
kwargs on every request, so it can force built-in tools on for an engine
regardless of what is saved in the UI.

**The value must be the dict shape.** The pre-catalog list shape
(`{"built_in_tools":["web_search"]}`) is no longer honored - it silently
sends no tools AND overrides any UI selection (see precedence above). If you
find an SMSS still using the list shape, migrate it.

Minimal working examples per provider - the alias is the provider's
versioned tool type, from the catalog:

```
# OpenAI (direct or Azure) - Responses API engines
GLOBAL_PARAM_OVERRIDE  ... global_param_override={"built_in_tools":{"web_search":{"alias":"web_search"}}}

# Anthropic direct
... global_param_override={"built_in_tools":{"web_search":{"alias":"web_search_20260318"}}}

# Claude on Vertex (basic tool version only)
... global_param_override={"built_in_tools":{"web_search":{"alias":"web_search_20250305"}}}

# Gemini - the required map param IS the Tool field, so alias alone is NOT
# enough; include the param:
... global_param_override={"built_in_tools":{"web_search":{"alias":"google_search","params":[{"alias":"google_search","value":{}}]}}}
```

To set tool parameters, use the catalog's params shape with a `value` (or
`default`) per param:

```json
{"built_in_tools": {"web_search": {
  "alias": "web_search_20260318",
  "params": [
    {"alias": "max_uses", "value": 3},
    {"alias": "allowed_domains", "value": ["example.com"]}
  ]
}}}
```

Things to know before using the SMSS route:

- **It bypasses the catalog entirely.** The runtime never checks
  builtin-tools.json for SMSS-supplied tools, so you can push a tool the UI
  would not offer (e.g. web search on an Azure OpenAI engine, which has no
  catalog node). SEMOSS will send it; whether it works is the provider's
  call. Test one ask on a dev engine first - the override rides on every
  ask, so a tool the provider rejects fails every request on the engine,
  not just the ones that wanted it.
- OpenAI-family server tools are **Responses API** features. An engine on
  the chat-completions path will reject the tool object.
- Anthropic: never set both allowed_domains and blocked_domains (400). On
  `web_search_20260318`/`web_fetch_20260318`, `allowed_callers` defaults to
  code-execution-mediated search; models without programmatic tool calling
  need `allowed_callers` set to `["direct"]` or the request 400s.
- SMSS changes require an engine reload to take effect.

---

## 5. Adding a catalog entry - checklist

1. Pick the right node: direct provider, or host.modelProvider for
   multi-vendor hosts. Never mix shapes on one node.
2. The alias is the provider's exact (versioned) tool type string.
3. Params are the provider's request fields, verbatim. Provider constants
   the request needs (like anthropic `name`/`type`) go in as required params
   with `show_in_ui: false`.
4. Verify every `default` against the provider docs - defaults are sent
   live on every ask (see section 1).
5. Use `constraints` to gate by api/models/regions where the host restricts
   availability (`constraints.models` matches on model id with qualifier
   peeling, so `openai.gpt-5.4` matches region-prefixed ids).
6. Run one live ask with the tool enabled before merging.
7. Remember existing engine selections are snapshots - they keep the old
   definition until re-saved in the UI.

---

## 6. Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Tool never reaches the provider | `built_in_tools` is not the dict shape (legacy list normalizes to no tools, silently); or the engine is REST-backed (ride-along is python engines only) |
| UI-enabled tool stopped working after an SMSS edit | SMSS `global_param_override` has a `built_in_tools` key that overwrites the UI selection - fix or remove it |
| Every ask on the engine 400s | An invalid param default (providers validate tool objects up front), or the host does not support the tool (e.g. chat-completions engine, Bedrock + Anthropic tools) |
| Catalog fix did not take effect | Saved selections are snapshots: re-save the tool in the UI and reload the engine |
| DB/BUILTINTOOLS edit not picked up | Selection is captured at engine open - reload the engine |

Key code, if you need to trace further:

- Catalog + lookup: `prerna.util.StaticBuiltinToolsCatalog`,
  `prerna.reactor.model.GetModelBuiltinToolsReactor`
- Storage + validation: `prerna.auth.utils.SecurityModelMetadataUtils`
  (`normalizeBuiltinToolsProperty`, `parseStoredBuiltinTools`)
- Ride-along injection: `prerna.engine.impl.model.AbstractPythonModelEngine#askCall`
- Python normalization + builders:
  `py/genai_client/message_builders/semoss_base/builtin_tools.py` and each
  provider's message builder `_build_built_in_tools`
