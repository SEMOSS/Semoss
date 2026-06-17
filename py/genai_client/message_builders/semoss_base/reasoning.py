"""Shared reasoning / "thinking" normalization across provider message builders.

Java always sends two keys in the param map: ``thinking`` (bool) and ``effort``
(a canonical string). The providers we support split into two camps:

* effort-based ........ OpenAI, modern Anthropic (Opus 4.6+/Sonnet 4.6/Fable),
                        Gemini 3.x (``thinking_level``)
* token-budget-based .. Gemini 2.5 (``thinking_budget``), Bedrock Converse
                        (``reasoning_config.budget_tokens``), legacy Anthropic

``normalize_reasoning`` collapses the incoming param map (and, where available,
the SMSS ``ModelSettings``) into a single canonical result. Each builder then
maps that result onto its provider-native shape via the small ``effort``<->
``budget`` ladder below, branching on model family where needed.
"""

from typing import Any, Dict, Optional
from pydantic import BaseModel
from ...utils import string_to_bool

CANONICAL_EFFORTS = ("none", "minimal", "low", "medium", "high", "xhigh", "max")

DEFAULT_EFFORT = "medium"

MIN_BUDGET = 1024

EFFORT_TO_BUDGET = {
    "none": 0,
    "minimal": 512,
    "low": 4096,
    "medium": 8192,
    "high": 24576,
    "xhigh": 32768,
    "max": 63999,
}


def budget_to_effort(budget: Any) -> str:
    """Map an integer-ish token budget onto the canonical effort ladder."""
    try:
        n = int(budget)
    except (TypeError, ValueError):
        return DEFAULT_EFFORT
    if n <= 0:
        return "none"
    if n < 1024:
        return "minimal"
    if n < 6144:
        return "low"
    if n < 16384:
        return "medium"
    if n < 28672:
        return "high"
    if n < 49152:
        return "xhigh"
    return "max"


def effort_to_budget(effort: Optional[str]) -> int:
    """Map a canonical effort onto an approximate token budget."""
    return EFFORT_TO_BUDGET.get(
        (effort or DEFAULT_EFFORT).lower(), EFFORT_TO_BUDGET[DEFAULT_EFFORT]
    )


def _coerce_effort(value: Any) -> Optional[str]:
    """Return a canonical effort string, or None if the value isn't usable.

    Accepts a canonical string, a numeric token budget (str or int), and treats
    anything else as "not specified".
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        v = value.strip().lower()
        if v in CANONICAL_EFFORTS:
            return v
        if v.lstrip("-").isdigit():
            return budget_to_effort(int(v))
        return None
    if isinstance(value, (int, float)):
        return budget_to_effort(int(value))
    return None


class ResolvedReasoning(BaseModel):
    """Canonical, provider-agnostic reasoning settings."""

    enabled: bool
    effort: str
    budget: int


def normalize_reasoning(
    param_map: Dict[str, Any],
    model_settings: Any = None,
    *,
    pop: bool = True,
) -> Optional[ResolvedReasoning]:
    """Resolve thinking/effort from the param map first, then SMSS settings.

    Returns ``None`` when reasoning is disabled (the builder should then emit the
    provider's "off"/omitted shape). When ``pop`` is True (default) the recognized
    keys (``thinking``, ``thinking_budget``, ``effort``) are removed from
    ``param_map`` so they never leak into the provider request body.
    """
    grab = param_map.pop if pop else param_map.get
    thinking_raw = grab("thinking", None)
    budget_raw = grab("thinking_budget", None)
    effort_raw = grab("effort", None)

    # ---- is thinking enabled? ----
    passthrough_budget = None
    if isinstance(thinking_raw, dict):
        # Claude Code / Anthropic passthrough: {"type": "enabled"|"adaptive"|
        # "disabled", "budget_tokens": N}
        enabled: Optional[bool] = thinking_raw.get("type") in ("enabled", "adaptive")
        passthrough_budget = thinking_raw.get("budget_tokens")
    elif thinking_raw is None:
        enabled = None
    else:
        enabled = string_to_bool(thinking_raw)

    if enabled is None and model_settings is not None:
        enabled = bool(getattr(model_settings, "thinking", False))
    enabled = bool(enabled)

    if not enabled:
        return None

    effort = _coerce_effort(effort_raw)
    if effort is None and model_settings is not None:
        effort = _coerce_effort(getattr(model_settings, "effort", None))
    if effort is None and passthrough_budget:
        effort = budget_to_effort(passthrough_budget)
    if effort is None and budget_raw is not None:
        effort = budget_to_effort(budget_raw)
    if effort is None:
        effort = DEFAULT_EFFORT

    budget = passthrough_budget or budget_raw
    if not budget and model_settings is not None:
        budget = getattr(model_settings, "thinking_budget", None)
    if not budget:
        budget = effort_to_budget(effort)
    try:
        budget = int(budget)
    except (TypeError, ValueError):
        budget = effort_to_budget(effort)
    budget = max(budget, MIN_BUDGET)

    return ResolvedReasoning(enabled=True, effort=effort, budget=budget)
