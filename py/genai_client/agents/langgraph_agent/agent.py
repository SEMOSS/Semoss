"""Build a LangGraph ``CompiledGraph`` from a SEMOSS workspace.

Two entry points:

* ``SemossAgent.from_config(cfg)`` — pure, unit-testable. The caller is
  responsible for supplying an already-resolved
  :class:`~genai_client.agents.langgraph_agent.config.SemossAgentConfig`
  (including any model wrapping).

* ``SemossAgent.from_workspace(workspace_id, ...)`` — convenience wrapper
  that fetches ``CONFIG_JSON`` for the workspace and any recursively
  referenced subagents via a Pixel loader.

Both return a ``CompiledGraph`` that behaves like any hand-authored
LangGraph agent: ``.invoke``, ``.stream``, ``.get_state``, composition,
LangSmith tracing.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, List, Literal, Optional

from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool, tool
from langgraph.prebuilt import create_react_agent

from .config import MCPRef, SemossAgentConfig, SubAgentRef
from .mcp_tools import load_mcp_tools

logger = logging.getLogger(__name__)


PixelLoader = Callable[[str], dict]
"""Callable that runs a Pixel string and returns the first output object."""


def _default_pixel_loader() -> PixelLoader:
    """Return the in-SEMOSS default loader backed by ``semoss.Insight``.

    Raises at call time (not import time) so external callers who supply
    their own loader do not pay the import cost.
    """

    def _run(pixel: str) -> dict:
        from semoss import Insight

        result = Insight().run_pixel(pixel=pixel, raw=True)
        # `raw=True` returns the full pixelReturn envelope.
        outputs = result[0].get("pixelReturn") if result else None
        if not outputs:
            raise RuntimeError(
                f"Pixel returned no output: {pixel!r}. This usually means the "
                "workspace/engine id is wrong or the current user lacks access."
            )
        first = outputs[-1].get("output")
        return first if isinstance(first, dict) else {"value": first}

    return _run


def _wrap_model(model: Any) -> BaseChatModel:
    """Coerce ``model`` into a LangChain chat model.

    Accepts an already-wrapped chat model, a SEMOSS ``ModelEngine`` (via
    duck-typed ``to_langchain_chat_model``), or a model engine id string
    (which triggers a ``ModelEngine`` instantiation).
    """

    if isinstance(model, BaseChatModel):
        return model
    if hasattr(model, "to_langchain_chat_model"):
        return model.to_langchain_chat_model()
    if isinstance(model, str):
        from gaas_gpt_model import ModelEngine

        return ModelEngine(engine_id=model).to_langchain_chat_model()
    raise TypeError(
        f"Unsupported model type: {type(model).__name__}. "
        "Expected BaseChatModel, ModelEngine, or engine_id string."
    )


def _parse_config_json(raw: Any) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, str):
        try:
            return json.loads(raw) or {}
        except json.JSONDecodeError as e:
            logger.warning(
                "Workspace CONFIG_JSON failed to parse (%s); subagents/mode "
                "from it will be ignored. Raw prefix: %r",
                e, raw[:120] if len(raw) > 120 else raw,
            )
            return {}
    if isinstance(raw, dict):
        return raw
    logger.warning(
        "Workspace CONFIG_JSON has unexpected type %s; expected str or dict.",
        type(raw).__name__,
    )
    return {}


def _config_from_workspace_output(output: dict) -> dict:
    """Extract the adapter-relevant fields from a ``GetWorkspace`` output."""

    config_json = _parse_config_json(output.get("config_json"))
    return {
        "workspace_id": output.get("workspace_id") or output.get("id"),
        "name": output.get("name"),
        "description": output.get("description"),
        "system_prompt": output.get("system_prompt"),
        "mcps": output.get("mcp") or [],
        "subagents": config_json.get("subagents") or [],
        "mode": (config_json.get("mode") or "react").lower(),
        "model_id": output.get("model_id") or config_json.get("model_id"),
    }


def _build_subagent_tool(
    ref: SubAgentRef,
    child_graph: Any,
) -> BaseTool:
    alias = ref.alias
    description = (
        ref.description
        or f"Delegate the task to the '{alias}' subagent and return its final answer."
    )

    @tool(alias, description=description)
    def _delegate(task: str) -> str:
        result = child_graph.invoke({"messages": [{"role": "user", "content": task}]})
        messages = result.get("messages", []) if isinstance(result, dict) else []
        if not messages:
            return ""
        last = messages[-1]
        return getattr(last, "content", None) or (
            last.get("content", "") if isinstance(last, dict) else ""
        )

    return _delegate


def _build_deep_subagents(
    subagents: List[SubAgentRef],
    load_pixel: PixelLoader,
    access_key: Optional[str],
    secret_key: Optional[str],
    room_id: Optional[str],
) -> List[dict]:
    """Materialize SEMOSS subagents into the dict shape deepagents wants."""

    out: List[dict] = []
    for ref in subagents:
        ws = load_pixel(f'GetWorkspace(workspaceId=["{ref.workspace_id}"]);')
        fields = _config_from_workspace_output(ws)
        child_mcps = [MCPRef(**m) for m in fields.get("mcps") or []]
        child_tools = load_mcp_tools(child_mcps, access_key, secret_key, room_id)
        out.append(
            {
                "name": ref.alias,
                "description": ref.description or fields.get("description") or ref.alias,
                "prompt": fields.get("system_prompt") or "",
                "tools": child_tools,
            }
        )
    return out


class SemossAgent:
    """Namespace of factory methods that return LangGraph ``CompiledGraph``s."""

    @staticmethod
    def from_config(
        config: SemossAgentConfig,
        *,
        pixel_loader: Optional[PixelLoader] = None,
        _depth: int = 0,
    ) -> Any:
        if config.model is None:
            raise ValueError("SemossAgentConfig.model is required.")
        model = _wrap_model(config.model)
        tools = load_mcp_tools(
            config.mcps, config.access_key, config.secret_key, config.room_id
        )

        if config.subagents and _depth >= config.max_subagent_depth:
            logger.info(
                "Subagent depth cap reached at depth=%d; skipping %d subagent(s).",
                _depth,
                len(config.subagents),
            )
        elif config.subagents:
            for ref in config.subagents:
                child = SemossAgent.from_workspace(
                    workspace_id=ref.workspace_id,
                    access_key=config.access_key,
                    secret_key=config.secret_key,
                    room_id=config.room_id,
                    max_subagent_depth=config.max_subagent_depth,
                    pixel_loader=pixel_loader,
                    _depth=_depth + 1,
                )
                tools.append(_build_subagent_tool(ref, child))

        if config.mode == "deep":
            return _build_deep_graph(config, model, tools, pixel_loader)

        return create_react_agent(
            model=model,
            tools=tools,
            prompt=config.system_prompt,
        )

    @staticmethod
    def from_workspace(
        workspace_id: str,
        *,
        model: Any = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        room_id: Optional[str] = None,
        mode: Optional[Literal["react", "deep"]] = None,
        max_subagent_depth: int = 1,
        pixel_loader: Optional[PixelLoader] = None,
        _depth: int = 0,
    ) -> Any:
        loader = pixel_loader or _default_pixel_loader()
        raw = loader(f'GetWorkspace(workspaceId=["{workspace_id}"]);')
        fields = _config_from_workspace_output(raw)

        resolved_model = model
        if resolved_model is None:
            model_id = fields.get("model_id")
            if not model_id:
                raise ValueError(
                    f"Workspace {workspace_id} has no model configured and none was passed."
                )
            resolved_model = model_id

        cfg = SemossAgentConfig(
            workspace_id=fields["workspace_id"],
            name=fields.get("name"),
            description=fields.get("description"),
            system_prompt=fields.get("system_prompt"),
            model=resolved_model,
            mcps=[MCPRef(**m) for m in fields.get("mcps") or []],
            subagents=[
                SubAgentRef.model_validate(s) for s in fields.get("subagents") or []
            ],
            mode=(mode or fields.get("mode") or "react"),
            access_key=access_key,
            secret_key=secret_key,
            room_id=room_id,
            max_subagent_depth=max_subagent_depth,
        )
        return SemossAgent.from_config(cfg, pixel_loader=pixel_loader, _depth=_depth)


def _build_deep_graph(
    config: SemossAgentConfig,
    model: BaseChatModel,
    tools: List[BaseTool],
    pixel_loader: Optional[PixelLoader] = None,
) -> Any:
    try:
        from deepagents import create_deep_agent
    except ImportError as exc:  # pragma: no cover - guarded
        raise ImportError(
            "mode='deep' requires the 'deepagents' package. "
            "Install with: pip install deepagents"
        ) from exc

    deep_subagents = _build_deep_subagents(
        config.subagents,
        pixel_loader or _default_pixel_loader(),
        config.access_key,
        config.secret_key,
        config.room_id,
    )
    return create_deep_agent(
        tools=tools,
        model=model,
        instructions=config.system_prompt or "",
        subagents=deep_subagents,
    )


def build_agent(
    workspace_id: str,
    *,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    room_id: Optional[str] = None,
    mode: Optional[Literal["react", "deep"]] = None,
    model: Any = None,
) -> Any:
    """Shorthand for :meth:`SemossAgent.from_workspace`."""

    return SemossAgent.from_workspace(
        workspace_id=workspace_id,
        access_key=access_key,
        secret_key=secret_key,
        room_id=room_id,
        mode=mode,
        model=model,
    )
