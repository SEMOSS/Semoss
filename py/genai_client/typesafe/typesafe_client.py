"""Jev evaluations without SEMOSS chat messages or text-generation adapters."""

import base64
import ssl
from collections.abc import Mapping
from typing import Any

import httpx2
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from typesafe_sdk import (
    Choice,
    Image,
    JSONContent,
    Noul,
    Question,
    RetryPolicy,
    Score,
    TypeSafeClient,
)
from typesafe_sdk.constants import DEFAULT_TIMEOUT

# Image input is an opt-in API feature, per the SDK docs.
IMAGES_HEADER = "TypeSafe-Danger-Zone-Images"

# How images reach the server: "assets" is the SDK's multipart upload (TypeSafe's
# API); "images" is a JSON list of data URLs (vLLM and OpenJev /v1/systemone).
IMAGE_FORMATS = ("assets", "images")

# Data URLs need a type; servers decode by content, so a missed sniff only mislabels.
_IMAGE_SIGNATURES = {
    b"\x89PNG": "image/png",
    b"\xff\xd8\xff": "image/jpeg",
    b"GIF8": "image/gif",
    b"RIFF": "image/webp",
}


class TypeSafeCallOptions(BaseModel):
    """JSON-compatible transport options; the engine owns the model selection."""

    model_config = ConfigDict(extra="forbid", strict=True)

    timeout: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    max_retries: int | None = Field(default=None, ge=0)


class TypeSafeClientWrapper:
    _state_adapter = TypeAdapter(JSONContent)
    _question_types = {"choice": Choice, "score": Score, "noul": Noul}

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "jev-latest",
        base_url: str | None = None,
        timeout: float | None = None,
        max_retries: int = 2,
        image_format: str = "assets",
    ):
        if not isinstance(model, str) or not model.strip():
            raise ValueError("model must be a nonempty string")
        if image_format not in IMAGE_FORMATS:
            raise ValueError(f"image_format must be one of {IMAGE_FORMATS}")
        self.image_format = image_format
        options = TypeSafeCallOptions(timeout=timeout, max_retries=max_retries)
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.timeout = options.timeout
        self.max_retries = options.max_retries
        self._client = self._get_client()

    def _get_client(self) -> TypeSafeClient:
        http_client = httpx2.Client(
            verify=ssl.create_default_context(),
            timeout=self.timeout if self.timeout is not None else DEFAULT_TIMEOUT,
        )
        try:
            return TypeSafeClient(
                api_key=self.api_key,
                model=self.model,
                base_url=self.base_url,
                retry=RetryPolicy(max_retries=self.max_retries),
                http_client=http_client,
            )
        except Exception:
            http_client.close()
            raise

    def ask(
        self,
        state: JSONContent,
        questions: Mapping[str, Question],
        parameters: Mapping[str, Any] | None = None,
        usage_ceiling: Any = None,
        assets: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Evaluate named questions and return the complete JSON response body.

        Both SDK question objects and Pixel/JSON question dictionaries are accepted.
        Score criteria are an ordered list (SDK >= 0.6), with levels starting at zero.
        Noul values remain probabilities, and score legend/probability keys are JSON
        strings. SDK/HTTP objects never cross the Java/Python bridge.

        ``usage_ceiling`` is accepted like the text clients' keyword and not
        used: an evaluation has no output maximum to lower. It rides in the
        same keyword arguments as the request, since Java sends the whole call
        as one ``ask(**json.loads(...))`` expression; Java refuses an
        evaluation that does not fit before it runs, and bills what it used.

        ``assets`` maps the alias the state or questions use for an image, such
        as ``<image_1>``, to its base64 bytes. They are sent per ``image_format``.
        """
        state = self._state_adapter.validate_python(state, strict=True)
        normalized = self._normalize_questions(questions)
        options = TypeSafeCallOptions.model_validate(
            {} if parameters is None else parameters
        )
        retry = (
            RetryPolicy(max_retries=options.max_retries)
            if options.max_retries is not None
            else None
        )
        assets = dict(assets or {})
        # from_base64 rejects invalid data in either format
        images = {alias: Image.from_base64(data) for alias, data in assets.items()}
        send: dict[str, Any] = {}
        if images and self.image_format == "images":
            state = self._without_aliases(state, assets)
            send["extra_body"] = {"images": [self._data_url(data) for data in assets.values()]}
        elif images:
            send = {"assets": images, "extra_headers": {IMAGES_HEADER: "true"}}
        answer = self._client.system_one(
            state, normalized, timeout=options.timeout, retry=retry, **send
        )
        # The SDK validates known answer types but drops unknown answer kinds and
        # extra fields from model_dump(). The HTTP JSON body preserves them too.
        return answer.raw_http_response.json()

    @staticmethod
    def _without_aliases(state: Any, aliases: Mapping[str, str]) -> Any:
        """Drop alias items from a list state; `images` servers place images themselves."""
        # shortcut: only whole list items are removed, so an alias inside text stays as text
        if not isinstance(state, list):
            return state
        rest = [item for item in state if not (isinstance(item, str) and item in aliases)]
        return rest[0] if len(rest) == 1 and isinstance(rest[0], str) else rest

    @staticmethod
    def _data_url(data: str) -> str:
        head = base64.b64decode(data[:16])
        content_type = next(
            (kind for sig, kind in _IMAGE_SIGNATURES.items() if head.startswith(sig)),
            "image/png",
        )
        return f"data:{content_type};base64,{data}"

    @classmethod
    def _normalize_questions(
        cls, questions: Mapping[str, Question]
    ) -> dict[str, Choice | Score | Noul]:
        if not isinstance(questions, Mapping) or not questions:
            raise ValueError("questions must be a nonempty map of named questions")

        normalized = {}
        for name, question in questions.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError("question names must be nonempty strings")
            if isinstance(question, (Choice, Score, Noul)):
                parsed = question
            elif isinstance(question, Mapping):
                question_type = question.get("type")
                if (
                    not isinstance(question_type, str)
                    or question_type not in cls._question_types
                ):
                    raise ValueError(
                        f"Question {name!r} must have type 'choice', 'score', or 'noul'"
                    )
                parsed = cls._question_types[question_type].model_validate(
                    dict(question)
                )
            else:
                raise ValueError(f"Question {name!r} must be an SDK question or a map")

            if isinstance(parsed, (Choice, Score)) and not parsed.criteria:
                raise ValueError(f"Question {name!r} requires nonempty criteria")
            normalized[name] = parsed
        return normalized

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "TypeSafeClientWrapper":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
