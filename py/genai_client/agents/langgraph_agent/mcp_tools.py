"""SEMOSS MCPs → LangChain tools.

Uses ``langchain-mcp-adapters`` when available. The lookup happens lazily
so importing this module does not fail environments that do not have the
adapter installed; ``load_mcp_tools`` degrades to returning an empty list
with a warning in that case.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

from .config import MCPRef

logger = logging.getLogger(__name__)


def _bearer_headers(access_key: str, secret_key: str, room_id: str) -> dict:
    token = f"{access_key}:{secret_key}:room-{room_id}"
    return {"Authorization": f"Bearer {token}"}


def _server_name(mcp: MCPRef) -> str:
    return (mcp.name or "mcp").replace(" ", "_").lower()


async def _load_async(
    mcps: List[MCPRef],
    access_key: str,
    secret_key: str,
    room_id: str,
) -> List[Any]:
    from langchain_mcp_adapters.client import MultiServerMCPClient

    servers = {
        _server_name(mcp): {
            "url": mcp.url,
            "transport": "streamable_http",
            "headers": _bearer_headers(access_key, secret_key, room_id),
        }
        for mcp in mcps
    }
    client = MultiServerMCPClient(servers)
    return await client.get_tools()


def load_mcp_tools(
    mcps: List[MCPRef],
    access_key: Optional[str],
    secret_key: Optional[str],
    room_id: Optional[str],
) -> List[Any]:
    """Return LangChain ``BaseTool``s for the provided SEMOSS MCPs.

    Returns ``[]`` when there are no MCPs, when auth is missing, or when
    ``langchain-mcp-adapters`` is not installed.
    """

    if not mcps:
        return []
    if not (access_key and secret_key and room_id):
        logger.warning(
            "load_mcp_tools: skipping %d MCP(s); access_key/secret_key/room_id are required.",
            len(mcps),
        )
        return []

    try:
        import asyncio

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        coro = _load_async(mcps, access_key, secret_key, room_id)
        if loop is None:
            return asyncio.run(coro)

        # In a running loop the caller is responsible for awaiting; fall
        # back to a fresh loop in a thread to keep the sync API simple.
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
    except ImportError:
        logger.warning(
            "langchain-mcp-adapters is not installed; %d MCP(s) will be skipped.",
            len(mcps),
        )
        return []
    except Exception as exc:
        logger.warning("load_mcp_tools failed: %s", exc)
        return []
