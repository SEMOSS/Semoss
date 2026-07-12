"""Pydantic models mirroring the SEMOSS workspace agent config surface.

These are the fields the LangGraph adapter reads. They map onto Java
``AgentConfig`` / ``SubAgentSpec`` shapes and the ``WORKSPACE.CONFIG_JSON``
JSON blob populated by the workspace-editor UI.
"""

from __future__ import annotations

from typing import Any, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class MCPRef(BaseModel):
    """A SEMOSS MCP resource attached to a workspace.

    Auth-aware MCP servers are reached over HTTP with a bearer of
    ``<access_key>:<secret_key>:room-<room_id>`` (same pattern the
    Claude Code adapter uses).
    """

    url: str
    name: str
    type: Optional[str] = None
    description: Optional[str] = None

    model_config = ConfigDict(extra="ignore")


class SubAgentRef(BaseModel):
    """A named subagent declared in ``CONFIG_JSON.subagents[]``.

    Mirrors ``prerna.reactor.agent.config.SubAgentSpec``. Alias is the
    tool name the LLM sees; workspace_id points at another workspace
    whose config is loaded for the child run.
    """

    alias: str
    workspace_id: str = Field(..., alias="workspaceId")
    description: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class SemossAgentConfig(BaseModel):
    """Resolved agent config the adapter builds a ``CompiledGraph`` from.

    Only ``system_prompt`` and ``model`` are effectively required for a
    working react agent; every other field enriches the graph.

    ``model`` accepts a model engine id (``str``), a
    ``langchain_core.language_models.BaseChatModel`` instance, or a
    SEMOSS ``ModelEngine`` (which gets wrapped via
    ``ModelEngine.to_langchain_chat_model()``).

    ``mode`` chooses between vanilla ``create_react_agent`` and
    ``deepagents.create_deep_agent`` (planning + filesystem + subagents).
    """

    workspace_id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None

    system_prompt: Optional[str] = None
    model: Any = None

    mcps: List[MCPRef] = Field(default_factory=list)
    subagents: List[SubAgentRef] = Field(default_factory=list)

    mode: Literal["react", "deep"] = "react"

    # Auth used to reach MCP servers and to fetch child workspaces
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    room_id: Optional[str] = None

    # Subagent depth guard. Mirrors AgentConfig.SubAgentSpawnPolicy default of 1.
    max_subagent_depth: int = 1

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")
