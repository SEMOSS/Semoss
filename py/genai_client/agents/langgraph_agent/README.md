# SEMOSS ↔ LangGraph adapter

Materialize a SEMOSS workspace agent config as a LangGraph
[`CompiledGraph`](https://langchain-ai.github.io/langgraph/reference/graphs/#compiledstategraph)
so anything that consumes LangGraph — LangSmith, LangGraph Studio, LangServe,
composition into another graph — can consume a SEMOSS-authored agent.

## Quickstart

```python
from genai_client.agents.langgraph_agent import SemossAgent

agent = SemossAgent.from_workspace(
    "93c85f32-1023-425d-8167-14111f26ceb4",
    access_key="...",
    secret_key="...",
    room_id="babae1e3-42cb-490d-a622-c06e6a59da54",
)

result = agent.invoke(
    {"messages": [{"role": "user", "content": "summarize the recent news"}]}
)

for chunk in agent.stream({"messages": [{"role": "user", "content": "..."}]}):
    print(chunk)
```

The returned object is a stock LangGraph `CompiledGraph` — anything you would
do with `create_react_agent(...)` works identically here.

## What the adapter maps

| SEMOSS | LangGraph |
| --- | --- |
| `WORKSPACE.system_prompt` | react agent `prompt` |
| `WORKSPACE.model_engine_id` | LangChain `BaseChatModel` via `ModelEngine.to_langchain_chat_model()` |
| `WORKSPACE.mcp[]` | `BaseTool`s via `langchain-mcp-adapters` |
| `CONFIG_JSON.subagents[]` | Child `CompiledGraph`s wrapped as delegate tools |
| `CONFIG_JSON.mode == "deep"` | Routed through `deepagents.create_deep_agent` |

## Deep mode

Set `mode="deep"` on the workspace's `CONFIG_JSON` (or override at build time)
to route through [`deepagents`](https://docs.langchain.com/oss/python/deepagents/overview).
The child gets a planning tool (TodoWrite-style), a virtual filesystem, and
its subagents materialized in deepagents' native format.

```python
agent = SemossAgent.from_workspace("...", mode="deep", ...)
```

Deep mode is entirely opt-in; workspaces without `mode` default to a plain
react agent.

## Configuration surface

`SemossAgentConfig` is a Pydantic model — use it directly when you want to
bypass the workspace fetch:

```python
from genai_client.agents.langgraph_agent import (
    SemossAgent,
    SemossAgentConfig,
    MCPRef,
    SubAgentRef,
)

cfg = SemossAgentConfig(
    system_prompt="You are a careful research assistant.",
    model=my_chat_model,        # BaseChatModel | ModelEngine | engine_id str
    mcps=[MCPRef(url="...", name="search")],
    subagents=[SubAgentRef(alias="researcher", workspaceId="ddd2a191-...")],
    mode="react",
    access_key="...", secret_key="...", room_id="...",
)
agent = SemossAgent.from_config(cfg)
```

## External usage

`from_workspace` fetches via `semoss.Insight().run_pixel(...)` by default,
which requires running inside a SEMOSS Python runtime. For external LangGraph
apps, supply a `pixel_loader` callable that hits the SEMOSS REST endpoint:

```python
def my_loader(pixel: str) -> dict:
    ...

agent = SemossAgent.from_workspace("...", pixel_loader=my_loader, ...)
```

## Depth guard

`max_subagent_depth` (default 1) mirrors
`AgentConfig.SubAgentSpawnPolicy.DEFAULT_MAX_SUBAGENT_DEPTH`. Increase only
if you understand the risk of unbounded delegation.
