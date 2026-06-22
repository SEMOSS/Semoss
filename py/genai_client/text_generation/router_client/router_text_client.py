from typing import Optional, Dict, Any, List, Union
import json
import re
import time
import warnings

from pydantic import BaseModel, Field

from ...constants import (
    AskModelEngineResponse2,
    TEMPLATE,
    TEMPLATE_NAME,
)
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ..model_engine_exception import ModelEngineException
from ...utils import string_to_bool


CANONICAL_STRATEGIES = ("complexity", "cost_weighted", "semantic")
DEFAULT_STRATEGY = "complexity"
DIFFICULTY_LABELS = ("trivial", "easy", "medium", "hard")


class RouterCandidate(BaseModel):
    engine_id: str
    provider: str
    model_name: str
    api_key: Optional[str] = None
    tier: Optional[str] = Field(default=None)
    input_cost_per_million: Optional[float] = None
    output_cost_per_million: Optional[float] = None
    extra_params: Optional[Dict[str, Any]] = None


class RouterTextClient(AbstractTextGenerationClient):

    def __init__(
        self,
        candidates: Union[str, List[Dict[str, Any]]],
        default_engine_id: str,
        strategy: Optional[str] = DEFAULT_STRATEGY,
        classifier_engine_id: Optional[str] = None,
        pin_to_first_choice: Optional[Union[str, bool]] = False,
        **kwargs,
    ):
        kwargs.setdefault("model_name", "semoss-router")
        super().__init__(
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
            **kwargs,
        )

        self.strategy = (strategy or DEFAULT_STRATEGY).lower()
        if self.strategy not in CANONICAL_STRATEGIES:
            raise ValueError(
                f"Unsupported router strategy '{strategy}'. "
                f"Supported: {', '.join(CANONICAL_STRATEGIES)}."
            )

        self.candidates = self._parse_candidates(candidates)
        if not self.candidates:
            raise ValueError("RouterClient requires at least one candidate model.")

        self._candidate_by_id: Dict[str, RouterCandidate] = {
            c.engine_id: c for c in self.candidates
        }

        if default_engine_id not in self._candidate_by_id:
            raise ValueError(
                f"default_engine_id '{default_engine_id}' is not among the configured candidates."
            )
        self.default_engine_id = default_engine_id

        self.classifier_engine_id = classifier_engine_id or default_engine_id
        if self.classifier_engine_id not in self._candidate_by_id:
            raise ValueError(
                f"classifier_engine_id '{self.classifier_engine_id}' is not among the configured candidates."
            )

        self._tier_index = self._build_tier_index()
        self._router = self._build_litellm_router()

        if isinstance(pin_to_first_choice, str):
            try:
                self._pin_to_first_choice = string_to_bool(pin_to_first_choice)
            except ValueError:
                self._pin_to_first_choice = False
        else:
            self._pin_to_first_choice = bool(pin_to_first_choice)
        self._pinned_engine_id: Optional[str] = None

        if not self._pin_to_first_choice:
            warnings.warn(
                "RouterClient is configured to re-classify and may switch child "
                "models per call. For multi-turn conversations this invalidates "
                "prompt caches and can mix provider-specific content blocks "
                "between turns. Set pin_to_first_choice=True (or create one "
                "router per room) to pick a child once and reuse it.",
                stacklevel=2,
            )

    @staticmethod
    def _parse_candidates(raw: Union[str, List[Dict[str, Any]]]) -> List[RouterCandidate]:
        if isinstance(raw, str):
            raw = json.loads(raw)
        return [RouterCandidate(**c) for c in raw]

    def _build_tier_index(self) -> Dict[str, List[str]]:
        buckets = {"small": [], "mid": [], "large": []}
        for c in self.candidates:
            tier = (c.tier or "mid").lower()
            if tier in buckets:
                buckets[tier].append(c.engine_id)
            else:
                buckets["mid"].append(c.engine_id)
        return {
            "trivial": buckets["small"] or buckets["mid"] or buckets["large"],
            "easy": buckets["small"] or buckets["mid"] or buckets["large"],
            "medium": buckets["mid"] or buckets["large"] or buckets["small"],
            "hard": buckets["large"] or buckets["mid"] or buckets["small"],
        }

    def _build_litellm_router(self):
        from litellm import Router

        model_list = []
        for c in self.candidates:
            params: Dict[str, Any] = {
                "model": self._litellm_model_string(c),
            }
            if c.api_key:
                params["api_key"] = c.api_key
            if c.extra_params:
                params.update(c.extra_params)
            model_list.append(
                {
                    "model_name": c.engine_id,
                    "litellm_params": params,
                }
            )
        return Router(model_list=model_list)

    @staticmethod
    def _litellm_model_string(c: RouterCandidate) -> str:
        provider = c.provider.lower()
        if provider == "openai":
            return c.model_name
        return f"{provider}/{c.model_name}"

    def _filter_accessible(
        self, candidate_ids: List[str], accessible: Optional[List[str]]
    ) -> List[str]:
        if not accessible:
            return list(candidate_ids)
        allow = set(accessible)
        return [cid for cid in candidate_ids if cid in allow]

    def _resolve_default(self, accessible: Optional[List[str]]) -> Optional[str]:
        if accessible and self.default_engine_id not in set(accessible):
            for cid in self._candidate_by_id:
                if cid in set(accessible):
                    return cid
            return None
        return self.default_engine_id

    def _classify_difficulty(self, prompt: str) -> Dict[str, Any]:
        from litellm import completion

        classifier = self._candidate_by_id[self.classifier_engine_id]
        instruction = (
            "Classify the difficulty of this request as exactly one word from: "
            "trivial, easy, medium, hard. Consider how much code, state, and "
            "edge-case handling it needs. Respond with ONLY the one word.\n\n"
            f"Request:\n{prompt}"
        )
        t0 = time.perf_counter()
        try:
            response = completion(
                model=self._litellm_model_string(classifier),
                api_key=classifier.api_key,
                messages=[
                    {"role": "system", "content": "You are a fast request triage classifier."},
                    {"role": "user", "content": instruction},
                ],
                max_tokens=16,
            )
            elapsed = time.perf_counter() - t0
            text = (response.choices[0].message.content or "").strip().lower()
            label = re.sub(r"[^a-z]", "", text)
            if label not in DIFFICULTY_LABELS:
                label = next(
                    (w for w in DIFFICULTY_LABELS if w in text),
                    "medium",
                )
            usage = response.usage
            return {
                "label": label,
                "classifier_engine_id": classifier.engine_id,
                "classifier_latency_s": round(elapsed, 3),
                "classifier_input_tokens": getattr(usage, "prompt_tokens", None),
                "classifier_output_tokens": getattr(usage, "completion_tokens", None),
            }
        except Exception as exc:
            return {
                "label": "medium",
                "classifier_engine_id": classifier.engine_id,
                "classifier_error": repr(exc)[:200],
            }

    def _pick_by_complexity(
        self, prompt: str, accessible: Optional[List[str]]
    ) -> Dict[str, Any]:
        decision = self._classify_difficulty(prompt)
        ordered = self._tier_index.get(decision["label"], [])
        filtered = self._filter_accessible(ordered, accessible)
        if filtered:
            return {"engine_id": filtered[0], **decision}
        return {"engine_id": None, **decision}

    def _pick_by_cost(self, accessible: Optional[List[str]]) -> Dict[str, Any]:
        accessible_ids = self._filter_accessible(
            [c.engine_id for c in self.candidates], accessible
        )
        priced = [
            c for c in self.candidates
            if c.engine_id in accessible_ids and c.input_cost_per_million is not None
        ]
        if not priced:
            return {"engine_id": None, "reason": "no_priced_candidates"}
        cheapest = min(priced, key=lambda c: c.input_cost_per_million or 0.0)
        return {"engine_id": cheapest.engine_id, "reason": "lowest_input_cost"}

    def _pick(
        self, prompt: str, accessible: Optional[List[str]]
    ) -> Dict[str, Any]:
        if self.strategy == "complexity":
            return self._pick_by_complexity(prompt, accessible)
        if self.strategy == "cost_weighted":
            return self._pick_by_cost(accessible)
        return self._pick_by_complexity(prompt, accessible)

    @staticmethod
    def _extract_user_prompt(messages: List[Dict[str, Any]]) -> str:
        for msg in reversed(messages):
            if msg.get("role") == "user":
                content = msg.get("content", "")
                if isinstance(content, list):
                    parts = [
                        part.get("text", "")
                        for part in content
                        if isinstance(part, dict) and part.get("type") == "text"
                    ]
                    return "\n".join(p for p in parts if p)
                return str(content) if content else ""
        return ""

    def _build_messages(self, **kwargs) -> List[Dict[str, Any]]:
        if "messages" in kwargs and isinstance(kwargs["messages"], list):
            return kwargs["messages"]
        if "message_json" in kwargs:
            raw = kwargs["message_json"]
            return json.loads(raw) if isinstance(raw, str) else raw
        question = kwargs.get("question") or kwargs.get("prompt") or ""
        system = kwargs.get("system") or kwargs.get("context")
        messages: List[Dict[str, Any]] = []
        if system:
            messages.append({"role": "system", "content": str(system)})
        if question:
            messages.append({"role": "user", "content": str(question)})
        return messages

    def ask_call(self, prefix: str = "", **kwargs) -> AskModelEngineResponse2:
        accessible_raw = kwargs.pop("accessible_engine_ids", None)
        accessible: Optional[List[str]] = None
        if accessible_raw is not None:
            accessible = (
                json.loads(accessible_raw)
                if isinstance(accessible_raw, str)
                else list(accessible_raw)
            )

        messages = self._build_messages(**kwargs)
        if not messages:
            raise ValueError("RouterClient.ask requires messages, message_json, or question.")

        user_prompt = self._extract_user_prompt(messages)
        served_by_pin = False
        if (
            self._pin_to_first_choice
            and self._pinned_engine_id is not None
            and (not accessible or self._pinned_engine_id in set(accessible))
        ):
            pick = {"engine_id": self._pinned_engine_id, "reason": "pinned"}
            served_by_pin = True
        else:
            pick = self._pick(user_prompt, accessible)
        picked_id = pick.get("engine_id")
        acl_fallback = False
        if picked_id is None or (accessible and picked_id not in set(accessible)):
            fallback_id = self._resolve_default(accessible)
            if fallback_id is None:
                raise ModelEngineException(
                    "No router candidate is accessible to the current user."
                )
            acl_fallback = picked_id is not None and picked_id != fallback_id
            picked_id = fallback_id

        candidate = self._candidate_by_id[picked_id]
        forward_kwargs = self._extract_forward_kwargs(kwargs)

        t0 = time.perf_counter()
        from litellm import completion

        response = completion(
            model=self._litellm_model_string(candidate),
            api_key=candidate.api_key,
            messages=messages,
            **forward_kwargs,
        )
        elapsed = time.perf_counter() - t0

        text = response.choices[0].message.content or ""
        usage = response.usage
        prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
        completion_tokens = getattr(usage, "completion_tokens", 0) or 0

        if self._pin_to_first_choice and self._pinned_engine_id is None:
            self._pinned_engine_id = candidate.engine_id

        route_record = {
            "type": "router_trace",
            "strategy": self.strategy,
            "served_by_engine_id": candidate.engine_id,
            "served_by_provider": candidate.provider,
            "served_by_model": candidate.model_name,
            "underlying_model": getattr(response, "model", candidate.model_name),
            "latency_s": round(elapsed, 3),
            "acl_fallback": acl_fallback,
            "pin_to_first_choice": self._pin_to_first_choice,
            "served_by_pin": served_by_pin,
            "pinned_engine_id": self._pinned_engine_id,
            "decision": {
                k: v for k, v in pick.items()
                if k in (
                    "label", "reason", "classifier_engine_id",
                    "classifier_latency_s", "classifier_input_tokens",
                    "classifier_output_tokens", "classifier_error",
                )
            },
            "child_usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
            },
        }

        return AskModelEngineResponse2(
            response=text,
            prompt_tokens=prompt_tokens,
            response_tokens=completion_tokens,
            parts=[route_record],
        )

    @staticmethod
    def _extract_forward_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
        allowed = (
            "max_tokens", "max_completion_tokens", "temperature",
            "top_p", "top_k", "stop", "stop_sequences",
            "stream", "user", "metadata", "response_format",
            "tools", "tool_choice",
        )
        forward: Dict[str, Any] = {}
        for key in allowed:
            if key in kwargs and kwargs[key] is not None:
                forward[key] = kwargs[key]
        return forward
