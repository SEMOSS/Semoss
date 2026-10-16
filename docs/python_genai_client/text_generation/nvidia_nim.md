# NVIDIA NIM text generation

NIM uses the existing `openai` Python SDK through `genai_client.OpenAiClient`.
No NVIDIA-specific Python dependency is needed. In the NVIDIA NIM import form,
set the served model ID, API base URL (including `/v1`), API key, and API mode
supported by that deployment. Use `EMPTY` as the key only for an endpoint that
does not require authentication.

```python
import os
import genai_client

client = genai_client.OpenAiClient(
    endpoint="https://integrate.api.nvidia.com/v1",
    model_name="poolside/laguna-xs-2.1",
    api_key=os.environ["NVIDIA_API_KEY"],
    chat_type="chat-completion",
    preserve_reasoning_content=True,
    chat_reasoning_mode="chat_template",
)
```

Use the actual model ID exposed by the chosen endpoint. Model availability,
tools, structured output, modalities, and reasoning controls depend on the
model and deployment. Set those capabilities in the existing model metadata
fields; choosing NIM does not imply that every model supports every feature.
The hosted endpoint normally uses `chat-completion`. Choose `responses` or
`completions` only when the deployment supports that API.

## Reasoning controls

`chat_reasoning_mode` affects Chat Completions only:

| Value | Behavior |
| --- | --- |
| `reasoning_effort` (default) | Existing OpenAI behavior: translate SEMOSS thinking/effort into `reasoning_effort`; omit temperature when reasoning is enabled. |
| `chat_template` | Send `chat_template_kwargs.enable_thinking` through the SDK's `extra_body`, using SEMOSS's thinking toggle. Preserve temperature and other template options. For models such as Laguna that expose this switch. |
| `none` | Consume SEMOSS reasoning controls without sending a reasoning parameter. For endpoints with no configurable reasoning control. |

`preserve_reasoning_content=True` captures the provider's `reasoning_content`
or `reasoning` field in streamed and nonstreamed Chat Completions, including tool
calls. It stores
that text as a THINKING part whose `source` records the original field name,
and replays it under that field on subsequent assistant turns. This preserves
the reasoning context required
by models that interleave reasoning and tools. Provider summaries and unmarked
legacy THINKING parts are not replayed as raw reasoning. Responses API requests
also do not receive this Chat Completions field.

Both settings are opt-in for existing clients: `preserve_reasoning_content`
defaults to `False`, and `chat_reasoning_mode` defaults to `reasoning_effort`.
Existing OpenAI configurations retain their behavior. New NIM imports enable
reasoning-content preservation; choose the reasoning control supported by the
individual model. As before, `global_param_override` takes final precedence
over generated request parameters.

Existing saved NIM configurations are not rewritten by this change. To update
one, use its real `MODEL` and `ENDPOINT` in the init script, remove obsolete
model-specific prompt templates, and add the appropriate settings above.
Deploy the Java and Python changes together so persisted THINKING parts retain
their source marker across history reloads.
