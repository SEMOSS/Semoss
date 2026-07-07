"""SEMOSS ↔ LangGraph adapter.

Materializes a SEMOSS workspace configuration as a LangGraph
``CompiledGraph`` so that ``langgraph``, ``langsmith`` and downstream
tooling can consume a SEMOSS-authored agent without knowing SEMOSS is the
source of truth.

Usage (in-SEMOSS)::

    from genai_client.agents.langgraph_agent import SemossAgent

    agent = SemossAgent.from_workspace(
        "93c85f32-1023-425d-8167-14111f26ceb4",
        access_key="...",
        secret_key="...",
        room_id="babae1e3-...",
    )
    result = agent.invoke({"messages": [{"role": "user", "content": "hi"}]})

Deep-mode (planning tool + virtual filesystem + subagents) via
``mode="deep"`` on the workspace config or overridden at build time.
"""

from .agent import SemossAgent, build_agent
from .config import MCPRef, SemossAgentConfig, SubAgentRef

__all__ = [
    "SemossAgent",
    "SemossAgentConfig",
    "MCPRef",
    "SubAgentRef",
    "build_agent",
]
