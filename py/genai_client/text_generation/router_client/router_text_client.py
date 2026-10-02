from typing import Optional, Dict, Any, List, Union
import json
import re
import time
import warnings

import genai_client
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
    client_class: str
    client_kwargs: Dict[str, Any] = Field(default_factory=dict)
    tier: Optional[str] = Field(default=None)
    input_cost_per_million: Optional[float] = None
    output_cost_per_million: Optional[float] = None


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
        self._clients: Dict[str, Any] = {}
        for c in self.candidates:
            self._clients[c.engine_id] = self._instantiate_client(c)

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

    @staticmethod
    def _instantiate_client(c: RouterCandidate):
        try:
            client_cls = getattr(genai_client, c.client_class)
        except AttributeError as exc:
            raise ValueError(
                f"Router candidate '{c.engine_id}' references unknown client_class "
                f"'{c.client_class}'. Must be a class exported from genai_client."
            ) from exc
        if client_cls is RouterTextClient or c.client_class == "RouterClient":
            raise ValueError(
                f"Router candidate '{c.engine_id}' cannot itself be a RouterClient."
            )
        return client_cls(**c.client_kwargs)

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

    @staticmethod
    def _build_classifier_message_json(prompt: str) -> str:
        instruction = (
            "Classify the difficulty of this request as exactly one word from: "
            "trivial, easy, medium, hard. Consider how much code, state, and "
            "edge-case handling it needs. Respond with ONLY the one word.\n\n"
            f"Request:\n{prompt}"
        )
        return json.dumps([
            {
                "type": "INPUT_TEXT",
                "schemaVersion": 2,
                "io": "INPUT",
                "parts": [
                    {"type": "SYSTEM", "prompt": "You are a fast request triage classifier."},
                    {"type": "TEXT", "text": instruction, "uiText": instruction},
                ],
            },
        ])

    @staticmethod
    def _response_text(ask_result: Any) -> str:
        if isinstance(ask_result, dict):
            resp = ask_result.get("response")
            return resp if isinstance(resp, str) else ""
        return str(ask_result or "")

    @staticmethod
    def _response_tokens(ask_result: Any) -> Dict[str, int]:
        if not isinstance(ask_result, dict):
            return {"prompt_tokens": 0, "completion_tokens": 0}
        return {
            "prompt_tokens": int(
                ask_result.get("numberOfTokensInPrompt")
                or ask_result.get("prompt_tokens")
                or 0
            ),
            "completion_tokens": int(
                ask_result.get("numberOfTokensInResponse")
                or ask_result.get("response_tokens")
                or 0
            ),
        }

    def _classify_difficulty(self, prompt: str) -> Dict[str, Any]:
        classifier = self._candidate_by_id[self.classifier_engine_id]
        client = self._clients[self.classifier_engine_id]
        msg_json = self._build_classifier_message_json(prompt)
        t0 = time.perf_counter()
        try:
            result = client.ask(message_json=msg_json, max_tokens=16)
            elapsed = time.perf_counter() - t0
            text = self._response_text(result).strip().lower()
            label = re.sub(r"[^a-z]", "", text)
            if label not in DIFFICULTY_LABELS:
                label = next(
                    (w for w in DIFFICULTY_LABELS if w in text),
                    "medium",
                )
            tokens = self._response_tokens(result)
            return {
                "label": label,
                "classifier_engine_id": classifier.engine_id,
                "classifier_client_class": classifier.client_class,
                "classifier_latency_s": round(elapsed, 3),
                "classifier_input_tokens": tokens["prompt_tokens"],
                "classifier_output_tokens": tokens["completion_tokens"],
            }
        except Exception as exc:
            return {
                "label": "medium",
                "classifier_engine_id": classifier.engine_id,
                "classifier_client_class": classifier.client_class,
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
    def _extract_user_prompt_from_message_json(message_json_raw: Union[str, list]) -> str:
        try:
            data = json.loads(message_json_raw) if isinstance(message_json_raw, str) else message_json_raw
        except Exception:
            return ""
        if not isinstance(data, list):
            return ""
        chunks: List[str] = []
        for msg in data:
            if not isinstance(msg, dict) or msg.get("io") != "INPUT":
                continue
            for part in msg.get("parts") or []:
                if not isinstance(part, dict):
                    continue
                if part.get("type") == "TEXT" and part.get("text"):
                    chunks.append(part["text"])
        return "\n".join(chunks)

    def ask_call(self, prefix: str = "", **kwargs) -> AskModelEngineResponse2:
        accessible_raw = kwargs.pop("accessible_engine_ids", None)
        accessible: Optional[List[str]] = None
        if accessible_raw is not None:
            accessible = (
                json.loads(accessible_raw)
                if isinstance(accessible_raw, str)
                else list(accessible_raw)
            )

        message_json = kwargs.get("message_json")
        if message_json:
            user_prompt = self._extract_user_prompt_from_message_json(message_json)
        else:
            user_prompt = str(kwargs.get("question") or kwargs.get("prompt") or "")
        if not user_prompt.strip():
            raise ValueError("RouterClient.ask requires a user prompt (message_json or question).")

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
        client = self._clients[picked_id]

        t0 = time.perf_counter()
        result = client.ask(**kwargs)
        elapsed = time.perf_counter() - t0

        text = self._response_text(result)
        tokens = self._response_tokens(result)
        prompt_tokens = tokens["prompt_tokens"]
        completion_tokens = tokens["completion_tokens"]

        if self._pin_to_first_choice and self._pinned_engine_id is None:
            self._pinned_engine_id = candidate.engine_id

        route_record = {
            "type": "router_trace",
            "strategy": self.strategy,
            "served_by_engine_id": candidate.engine_id,
            "served_by_client_class": candidate.client_class,
            "served_by_model": candidate.client_kwargs.get("model_name"),
            "latency_s": round(elapsed, 3),
            "acl_fallback": acl_fallback,
            "pin_to_first_choice": self._pin_to_first_choice,
            "served_by_pin": served_by_pin,
            "pinned_engine_id": self._pinned_engine_id,
            "decision": {
                k: v for k, v in pick.items()
                if k in (
                    "label", "reason", "classifier_engine_id",
                    "classifier_client_class",
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
