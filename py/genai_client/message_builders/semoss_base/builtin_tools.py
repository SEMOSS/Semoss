"""Shared normalization for the ``built_in_tools`` param across providers.

Java rides the engine's saved built-in tool selection along on every ask call.
There is exactly one shape, originating in the MODELMETADATA.BUILTINTOOLS
column: a dict keyed by canonical tool name whose values are the
meta/builtin-tools.json catalog definition for the tool, with a ``value``
added beside ``default`` on any parameter the user changed::

    {"web_search": {
        "alias": "web_search_20260318",
        "display_name": "Web Search",
        "params": [{"alias": "max_uses", "default": 5,
                    "value": 3, "show_in_ui": True}, ...]}}

A hand-written override can be as small as
``{"web_search": {"alias": "web_search_20260318"}}`` - the alias is what the
providers key their tool type on, so it must be present and versioned where
the provider versions its tools.

Every consumer should go through ``normalize_built_in_tools`` (or the small
helpers on top of it) rather than iterating the raw value; anything that is
not the dict shape normalizes to no tools.

The catalog is written so that a tool's parameters ARE the provider request
fields (anthropic carries its ``name``/``type`` as hidden required params,
google's single param names the Tool field, openai's params are the tool
object's fields). Builders therefore construct their provider-native specs
from ``built_in_tool_request_fields`` plus the ``alias`` - there are no
name tables to keep in sync with the catalog.
"""

from typing import Any, Dict, List


def normalize_built_in_tools(built_in_tools: Any) -> List[Dict[str, Any]]:
    """Collapse the ``built_in_tools`` dict into a list of
    ``{"name": str, "alias": str, "params": {alias: value}}`` selections.

    ``params`` holds the effective value per parameter: the user's ``value``
    when one was set, the catalog ``default`` otherwise. Anything that is not
    the dict shape normalizes to no tools rather than raising - a malformed
    selection should degrade the same way an unknown tool name does
    downstream.
    """
    if not isinstance(built_in_tools, dict):
        return []

    selections = []
    for name, config in built_in_tools.items():
        config = config if isinstance(config, dict) else {}
        selections.append(_to_selection(str(name), config))
    return selections


def built_in_tool_names(built_in_tools: Any) -> List[str]:
    """Just the canonical tool names, in selection order."""
    return [
        selection["name"] for selection in normalize_built_in_tools(built_in_tools)
    ]


def has_built_in_tool(built_in_tools: Any, tool_name: str) -> bool:
    """Whether the selection includes the given canonical tool name."""
    target = tool_name.lower()
    return any(
        name.lower() == target for name in built_in_tool_names(built_in_tools)
    )


def built_in_tool_request_fields(selection: Dict[str, Any]) -> Dict[str, Any]:
    """The selection's resolved parameter values as provider request fields,
    with unset optional values (None, empty list, empty dict) omitted - a
    web_search with no domain filters must not send ``allowed_domains=[]``.
    ``False`` and ``0`` are meaningful values and are kept.

    Note google's tool-field params default to ``{}`` where the empty dict
    means "enabled with provider defaults" - the google builder reads the
    raw ``params`` instead of going through this pruning.
    """
    fields: Dict[str, Any] = {}
    for alias, value in selection.get("params", {}).items():
        if value is None:
            continue
        if isinstance(value, (list, dict)) and len(value) == 0:
            continue
        fields[alias] = value
    return fields


def _to_selection(name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    alias = config.get("alias")
    return {
        "name": name,
        "alias": str(alias) if alias else name,
        "params": _selected_params(config.get("params")),
    }


def _selected_params(params: Any) -> Dict[str, Any]:
    """Effective value per parameter alias from a catalog-shaped params list."""
    selected: Dict[str, Any] = {}
    if isinstance(params, (list, tuple)):
        for param in params:
            if not isinstance(param, dict):
                continue
            alias = param.get("alias")
            if not alias:
                continue
            value = param.get("value", param.get("default"))
            if value is not None:
                selected[str(alias)] = value
    return selected
