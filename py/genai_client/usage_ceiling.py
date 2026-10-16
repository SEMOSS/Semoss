"""Fitting a model call inside the usage ceiling the Java engine sends.

When token or cost rules apply to a call, the Java engine adds a
``usage_ceiling`` keyword argument to ``ask`` and ``embeddings``::

    usage_ceiling = {
        "tokens": 120000,          # most tokens the call may use, prompt and output; or None
        "cost_micros": 2500000,    # most cost the call may use, millionths of a dollar; or None
        "rates": {                 # dollars per million tokens, when cost_micros is set
            "input": 2.5, "output": 10.0,
            "cache_read": 1.25, "cache_write": None, "reasoning": None,
        },
        "max_output_tokens": 8192, # the output maximum the call is reserved for; or None
        "min_output_tokens": 256,  # refuse when less than this is left for output
        "margin": 0.05,            # kept free when the prompt count is an estimate
    }

``tokens`` and ``cost_micros`` are what is left on the tightest rules; the
client fits the call in them with its own count of the prompt.
``max_output_tokens`` is the caller's output maximum, else the model's, else a
default; a request with no maximum of its own is capped at it.

The ceiling is a keyword argument of the call itself because the Java bridge
returns the value of only the last statement it runs, so each call from Java
is a single expression and nothing can be set up by a statement before it.

The text clients pop it in ``AbstractTextGenerationClient.ask`` and the
embedders in ``AbstractEmbedder.embeddings``, so it never reaches a provider.
For the length of one ``ask`` the ceiling is held in a context variable; each
client counts its finished provider request and calls
``fit_output_to_usage_ceiling`` (the Vertex clients through
``fit_max_new_tokens``), which ends in ``fit_to_usage_ceiling``, with the
output maximum the request carries. That either raises
``UsageLimitDoesNotFitError`` (the call is refused before the provider sees it, so
nothing is billed) or returns the output maximum to send.

Fitting a text call:

1. The prompt is counted as it will be sent, with the client's own
   tokenizer. Anything it cannot count exactly (media, tool definitions, a
   missing tokenizer) makes the count an estimate, and only an estimate keeps
   ``margin`` of what is left free.
2. The output left is the most output that fits in both ``tokens`` and
   ``cost_micros`` after the prompt.
3. With less output left than ``min_output_tokens`` the call is refused with
   ``USAGE_LIMIT_DOES_NOT_FIT``, so nothing is billed for an answer cut too
   short to use.
4. Otherwise the output maximum is the lesser of the output left and the caller's
   maximum, or ``max_output_tokens`` when the caller set none. It is never
   raised above what the caller or the model allows.
5. A thinking budget is lowered under that maximum (``fit_thinking_budget``),
   since providers count thinking inside the output maximum.
6. A response that stopped at a maximum the ceiling set is marked
   ``usageCapped``, so the caller can tell a cut answer from a short one.

Embeddings have no output: the input must fit (``input_fits``), priced at the
input rate.

Rates are dollars per million tokens, so tokens times a rate is a cost in
millionths of a dollar, the unit of ``cost_micros``.
"""

import base64
import binascii
import json
import math
import re
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Optional

from pydantic import BaseModel, field_validator

from .utils import sniff_image_mime, sniff_video_mime

USAGE_CEILING = "usage_ceiling"

# ErrorDetails.reason of a call refused because its prompt leaves too little for output
USAGE_LIMIT_DOES_NOT_FIT = "USAGE_LIMIT_DOES_NOT_FIT"
DOES_NOT_FIT_MESSAGE = "There is not enough left of your usage limit for this request"
DOES_NOT_FIT_CODE = 429
DOES_NOT_FIT_ERROR_TYPE = "Usage Limit"

DEFAULT_MIN_OUTPUT_TOKENS = 256
DEFAULT_MARGIN = 0.05

# No current model writes more than this in one response. A request that sets
# no output maximum keeps none when the output left is at least this large, since the
# provider's own maximum already fits; setting a value above a model's maximum
# is refused by most providers.
LARGEST_OUTPUT_MAXIMUM = 128_000

# Conservative estimates for what a text tokenizer cannot count. Text is
# estimated at one token for every three bytes of UTF-8, which is above what
# the current tokenizers produce for English, code and CJK text alike.
ESTIMATE_BYTES_PER_TOKEN = 3
IMAGE_TOKENS_ESTIMATE = 3_000
PDF_PAGE_TOKENS_ESTIMATE = 3_000
PDF_BYTES_PER_PAGE_ESTIMATE = 50_000
AUDIO_TOKENS_ESTIMATE = 5_000
VIDEO_TOKENS_ESTIMATE = 20_000
FILE_TOKENS_ESTIMATE = 5_000
BINARY_BYTES_PER_TOKEN_ESTIMATE = 4
# role and separator tokens chat formats add around each message and request
MESSAGE_OVERHEAD_TOKENS = 4
REQUEST_OVERHEAD_TOKENS = 3

# Content block types that carry one image, a file or audio, across the
# OpenAI chat and Responses, Anthropic and Bedrock request shapes.
_IMAGE_BLOCK_TYPES = {"image_url", "input_image", "image"}
_FILE_BLOCK_TYPES = {"file", "input_file", "document"}
_AUDIO_BLOCK_TYPES = {"input_audio", "audio"}

# Keys whose string values may be base64 or data URL payloads.
_MEDIA_KEYS = {"data", "file_data", "bytes", "image_url", "url", "base64", "base64Data"}
_RAW_BASE64_KEYS = {"data", "bytes", "base64", "base64Data"}
_PAYLOAD_KEYS = ("file_data", "data", "bytes", "base64", "base64Data")
_MIME_KEYS = ("media_type", "mime_type", "mimeType", "format", "filename", "name")

# Keys that are not billed as prompt text: replayed reasoning signatures and
# cache markers.
_SKIPPED_KEYS = {"signature", "thought_signature", "cache_control", "cachePoint"}

_BASE64_RE = re.compile(r"^[A-Za-z0-9+/=_\-\r\n]+$")
_PDF_PAGE_RE = re.compile(rb"/Type\s*/Page(?![A-Za-z])")

TextCounter = Callable[[str], int]


class UsageRates(BaseModel):
    """Prices of one model, in dollars per million tokens."""

    input: Optional[float] = None
    output: Optional[float] = None
    cache_read: Optional[float] = None
    cache_write: Optional[float] = None
    reasoning: Optional[float] = None

    model_config = {"extra": "ignore"}


@dataclass(frozen=True)
class PromptCount:
    """A prompt's token count; ``estimated`` when any part of it is a guess."""

    tokens: int
    estimated: bool


@dataclass(frozen=True)
class CeilingFit:
    """How a call fits its ceiling.

    ``output_left`` is the most output that fits, or None when no rule limits
    the output. ``max_tokens`` is the output maximum to send. ``lowered`` is
    True when the ceiling set or lowered that maximum, so the client sends it;
    the output left was what limited it when ``max_tokens`` equals
    ``output_left``.
    """

    prompt: PromptCount
    output_left: Optional[int]
    max_tokens: Optional[int]
    lowered: bool


class UsageLimitDoesNotFitError(ValueError):
    """The prompt leaves too little of the ceiling for the call to run."""

    def __init__(
        self,
        prompt: PromptCount,
        output_left: int,
        needed: int,
        embeddings: bool = False,
    ):
        self.prompt = prompt
        self.output_left = output_left
        self.needed = needed
        self.embeddings = embeddings
        super().__init__(does_not_fit_message(prompt, embeddings))

    @property
    def detail(self) -> str:
        return (
            f"prompt_tokens={self.prompt.tokens} estimated={self.prompt.estimated} "
            f"output_left={self.output_left} needed={self.needed}"
        )


def does_not_fit_message(prompt: PromptCount, embeddings: bool = False) -> str:
    """The refusal shown to the caller, naming how much the prompt uses."""
    if embeddings:
        return f"{DOES_NOT_FIT_MESSAGE}. The input uses about {prompt.tokens:,} tokens."
    return (
        f"{DOES_NOT_FIT_MESSAGE}. The prompt, including the conversation history, uses "
        f"about {prompt.tokens:,} tokens, which leaves too little for an answer."
    )


class UsageCeiling(BaseModel):
    """The most one call may use, as sent by the Java engine."""

    tokens: Optional[int] = None
    cost_micros: Optional[int] = None
    rates: Optional[UsageRates] = None
    max_output_tokens: Optional[int] = None
    min_output_tokens: int = DEFAULT_MIN_OUTPUT_TOKENS
    margin: float = DEFAULT_MARGIN

    model_config = {"extra": "ignore"}

    # a null value means the default, the same as a key left out
    @field_validator("min_output_tokens", mode="before")
    @classmethod
    def _default_min_output(cls, value: Any) -> Any:
        return DEFAULT_MIN_OUTPUT_TOKENS if value is None else value

    @field_validator("margin", mode="before")
    @classmethod
    def _default_margin(cls, value: Any) -> Any:
        return DEFAULT_MARGIN if value is None else value

    @classmethod
    def from_value(cls, value: Any) -> Optional["UsageCeiling"]:
        """Read the keyword argument: a dict, a JSON string or a UsageCeiling.

        Returns None when there is no ceiling, or when it limits neither
        tokens nor cost.
        """
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        if isinstance(value, UsageCeiling):
            ceiling = value
        elif isinstance(value, str):
            ceiling = cls.model_validate(json.loads(value))
        elif isinstance(value, Mapping):
            ceiling = cls.model_validate(dict(value))
        else:
            raise TypeError(
                f"{USAGE_CEILING} must be a dict or a JSON string, not {type(value).__name__}"
            )
        if ceiling.tokens is None and ceiling.cost_micros is None:
            return None
        return ceiling

    def _margin_fraction(self) -> float:
        # held between 0 and half, so a bad margin can neither add to what is
        # left nor take all of it
        return min(max(self.margin or 0.0, 0.0), 0.5)

    def _usable(self, amount: Optional[float], estimated: bool) -> Optional[float]:
        """The part of an amount a call may count on, less the margin when the
        prompt count is an estimate."""
        if amount is None:
            return None
        if estimated:
            # rounded so 10,000 less 5% is 9,500 and not 9,499.999...
            return round(amount * (1.0 - self._margin_fraction()), 6)
        return float(amount)

    def _rates(self) -> UsageRates:
        return self.rates or UsageRates()

    def _prompt_rate(self) -> float:
        """The rate a prompt is priced at before the call: the higher of the
        input and cache write rates, since a client such as Anthropic's marks
        the prompt for caching itself and a cache write costs more."""
        rates = self._rates()
        return max(rates.input or 0.0, rates.cache_write or 0.0)

    def output_left(self, prompt: PromptCount) -> Optional[int]:
        """The most output tokens that fit after the prompt, or None when no
        rule limits the output.

        Tokens: the ceiling minus the prompt. Cost: the ceiling minus the
        prompt priced as if nothing were cached, at the higher of the input and
        cache write rates, divided by the larger of the output and reasoning
        rates. When the prompt count is an estimate, the margin is kept free of
        both.
        """
        candidates = []
        tokens = self._usable(self.tokens, prompt.estimated)
        if tokens is not None:
            candidates.append(math.floor(tokens) - prompt.tokens)
        cost_left = self._cost_output_left(prompt)
        if cost_left is not None:
            candidates.append(cost_left)
        if not candidates:
            return None
        # the tighter of what tokens and cost leave is the one that holds
        return max(min(candidates), 0)

    def _cost_output_left(self, prompt: PromptCount) -> Optional[int]:
        """What is left of the cost ceiling, as output tokens, or None when cost
        does not limit the output. The output is priced at the larger of the
        output and reasoning rates, since any of it may be thinking."""
        budget = self._usable(self.cost_micros, prompt.estimated)
        if budget is None:
            return None
        rates = self._rates()
        left = budget - prompt.tokens * self._prompt_rate()
        if left <= 0:
            # the prompt alone uses all of the cost ceiling
            return 0
        per_output_token = max(rates.output or 0.0, rates.reasoning or 0.0)
        if per_output_token <= 0:
            # output costs nothing, so cost does not limit it
            return None
        return math.floor(left / per_output_token)

    def input_fits(self, prompt: PromptCount) -> bool:
        """True when an input with no output (embeddings) fits the ceiling.

        Embeddings are billed at the input rate and never cached, so the input
        is priced at it."""
        tokens = self._usable(self.tokens, prompt.estimated)
        if tokens is not None and prompt.tokens > tokens:
            return False
        budget = self._usable(self.cost_micros, prompt.estimated)
        if budget is not None and prompt.tokens * (self._rates().input or 0.0) > budget:
            return False
        return True

    def require_input_fits(self, prompt: PromptCount) -> None:
        """Raise UsageLimitDoesNotFitError when an embeddings input does not fit."""
        if not self.input_fits(prompt):
            raise UsageLimitDoesNotFitError(
                prompt, output_left=0, needed=prompt.tokens, embeddings=True
            )

    def fit_output(
        self, prompt: PromptCount, requested_max: Optional[int]
    ) -> CeilingFit:
        """Fit the output maximum inside what the prompt leaves.

        Raises UsageLimitDoesNotFitError when the output left is under
        ``min_output_tokens``. The maximum is never raised above the
        caller's. A request with no maximum gets the lesser of the output left
        and ``max_output_tokens``, the maximum the call is reserved for;
        without one it gets the output left, unless that is larger than any
        model's output maximum.
        """
        requested = _positive_int(requested_max)
        output_left = self.output_left(prompt)
        if output_left is None:
            return CeilingFit(
                prompt=prompt,
                output_left=None,
                max_tokens=requested,
                lowered=False,
            )
        # at least one token, so a prompt that leaves nothing for output is
        # always refused
        needed = max(int(self.min_output_tokens or 0), 1)
        if output_left < needed:
            raise UsageLimitDoesNotFitError(
                prompt, output_left=output_left, needed=needed
            )
        if requested is None:
            # Java reserved the call at max_output_tokens, so the output is held
            # to it even when more is left
            bounded = _positive_int(self.max_output_tokens)
            if bounded is not None:
                return CeilingFit(
                    prompt=prompt,
                    output_left=output_left,
                    max_tokens=min(output_left, bounded),
                    lowered=True,
                )
            # no maximum is sent when the provider's own already fits
            if output_left >= LARGEST_OUTPUT_MAXIMUM:
                return CeilingFit(
                    prompt=prompt,
                    output_left=output_left,
                    max_tokens=None,
                    lowered=False,
                )
            return CeilingFit(
                prompt=prompt,
                output_left=output_left,
                max_tokens=output_left,
                lowered=True,
            )
        # the caller's maximum is lowered to the output left, never raised
        if requested > output_left:
            return CeilingFit(
                prompt=prompt,
                output_left=output_left,
                max_tokens=output_left,
                lowered=True,
            )
        return CeilingFit(
            prompt=prompt,
            output_left=output_left,
            max_tokens=requested,
            lowered=False,
        )


def fit_thinking_budget(
    budget: int,
    max_tokens: int,
    min_output_tokens: int,
    smallest_budget: int,
    can_turn_off: bool = True,
) -> Optional[int]:
    """The thinking budget that fits under a lowered output maximum.

    Providers count thinking inside the output maximum, so the budget is kept
    at most ``max_tokens - min_output_tokens``, which leaves part of it for the
    answer and keeps it below the maximum, as Anthropic requires. Returns None
    when even the provider's smallest budget does not fit and thinking should
    be turned off; a provider that cannot turn thinking off gets its smallest
    budget instead.

    ``smallest_budget`` is the least the provider accepts, such as Anthropic's
    1,024; too small a budget runs the call without thinking rather than
    refusing it.
    """
    most = int(max_tokens) - max(int(min_output_tokens or 0), 1)
    if budget <= most:
        return budget
    if most >= smallest_budget:
        return most
    return None if can_turn_off else smallest_budget


def _positive_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


# ---------------------------------------------------------------------------
# The ceiling of the call in progress
# ---------------------------------------------------------------------------


class UsageCeilingCall:
    """One ``ask`` under a ceiling: the fit the client chose and how it ended."""

    def __init__(self, ceiling: UsageCeiling):
        self.ceiling = ceiling
        self.fit: Optional[CeilingFit] = None
        self.stopped_at_max = False

    def fit_output(
        self,
        prompt: PromptCount,
        requested_max: Optional[int],
        can_cap: bool = True,
    ) -> CeilingFit:
        """``UsageCeiling.fit_output``, kept for ``capped`` unless the provider
        has no output maximum to stop at (``can_cap`` False)."""
        fit = self.ceiling.fit_output(prompt, requested_max)
        if can_cap:
            self.fit = fit
        return fit

    def note_stop(self, stopped_at_max: bool) -> None:
        # only ever set, so one provider request of the ask that stopped at
        # the maximum is enough
        if stopped_at_max:
            self.stopped_at_max = True

    def capped(self, response_tokens: Optional[int]) -> bool:
        """True when the response stopped at an output maximum the ceiling set.

        The provider's stop reason decides when the client reported one;
        otherwise a response that used the whole lowered maximum counts. A
        maximum set from ``max_output_tokens`` with more left to spare is not the
        limit's doing.
        """
        fit = self.fit
        if fit is None or not fit.lowered or fit.max_tokens is None:
            return False
        if fit.output_left is not None and fit.max_tokens < fit.output_left:
            return False
        if self.stopped_at_max:
            return True
        return (response_tokens or 0) >= fit.max_tokens


# A context variable rather than an argument, so each client fits its request
# where it builds it without the ceiling passed through every method, and a
# call running on another thread never sees it.
_CURRENT_CEILING: ContextVar[Optional[UsageCeilingCall]] = ContextVar(
    "genai_client_usage_ceiling", default=None
)


@contextmanager
def usage_ceiling_scope(
    ceiling: Optional[UsageCeiling],
) -> Iterator[Optional[UsageCeilingCall]]:
    """Hold the ceiling for the length of one call. A None ceiling also hides
    any ceiling of an enclosing call."""
    call = UsageCeilingCall(ceiling) if ceiling is not None else None
    token = _CURRENT_CEILING.set(call)
    try:
        yield call
    finally:
        _CURRENT_CEILING.reset(token)


def current_usage_ceiling() -> Optional[UsageCeilingCall]:
    return _CURRENT_CEILING.get()


def fit_to_usage_ceiling(
    requested_max: Optional[int],
    count_prompt: Callable[[], PromptCount],
    can_cap: bool = True,
) -> Optional[CeilingFit]:
    """Fit the call in progress to its ceiling.

    Returns None when no ceiling applies; the prompt is counted only when one
    does. ``can_cap`` is False for a provider with no output maximum: the
    call is still refused when the prompt leaves nothing for output, but the response is
    never marked capped.
    """
    call = _CURRENT_CEILING.get()
    if call is None:
        return None
    return call.fit_output(count_prompt(), requested_max, can_cap=can_cap)


def note_output_stop(stopped_at_max: bool) -> None:
    """Record whether the provider stopped the response at its output maximum."""
    call = _CURRENT_CEILING.get()
    if call is not None:
        call.note_stop(stopped_at_max)


# ---------------------------------------------------------------------------
# Counting a prompt
# ---------------------------------------------------------------------------


def estimate_text_tokens(text: str) -> int:
    """A conservative token estimate for text no tokenizer counted."""
    if not text:
        return 0
    return math.ceil(len(text.encode("utf-8")) / ESTIMATE_BYTES_PER_TOKEN)


def count_prompt_tokens(
    parts: Iterable[Any],
    definitions: Iterable[Any] = (),
    count_text: Optional[TextCounter] = None,
    text_exact: bool = True,
    message_count: int = 0,
) -> PromptCount:
    """Count a provider request's prompt.

    ``parts`` are the system prompt, messages and other content as sent to the
    provider (dicts, lists, strings, bytes or pydantic models). Text is counted
    with ``count_text``, or estimated when there is none. Images, files and
    audio are estimated. ``definitions`` are tool definitions and output
    schemas, counted as their JSON and always treated as an estimate, since
    providers render them in their own format. ``text_exact`` is False when
    ``count_text`` only approximates the provider's tokenizer, which makes the
    whole count an estimate. ``message_count`` adds the per message overhead.
    """
    counter = _PromptCounter(count_text, text_exact)
    for part in parts:
        counter.add(part)
    for definition in definitions:
        counter.add_definition(definition)
    tokens = (
        counter.tokens
        + max(int(message_count or 0), 0) * MESSAGE_OVERHEAD_TOKENS
        + REQUEST_OVERHEAD_TOKENS
    )
    return PromptCount(tokens=tokens, estimated=counter.estimated)


def count_texts_with_tokenizer(tokenizer: Any, texts: Iterable[str]) -> PromptCount:
    """Count plain strings with a genai_client tokenizer, or estimate them when
    there is none or it fell back to counting words."""
    from .tokenizers.local_tokenizer import LocalWordCountTokenizer

    texts = [text if isinstance(text, str) else str(text) for text in texts]
    inner = getattr(tokenizer, "tokenizer", None)
    # a word count runs well under the real token count, so it is treated as
    # no tokenizer at all
    if (
        tokenizer is None
        or not hasattr(tokenizer, "count_tokens")
        or isinstance(tokenizer, LocalWordCountTokenizer)
        or isinstance(inner, LocalWordCountTokenizer)
    ):
        return PromptCount(sum(estimate_text_tokens(t) for t in texts), estimated=True)
    try:
        return PromptCount(
            sum(int(tokenizer.count_tokens(t)) for t in texts), estimated=False
        )
    except Exception:
        return PromptCount(sum(estimate_text_tokens(t) for t in texts), estimated=True)


def to_json_text(value: Any) -> str:
    """JSON text of a definition, for counting. Pydantic models and classes are
    rendered as their dumps and schemas."""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=_json_default)


def _json_default(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", exclude_none=True)
    if isinstance(value, type) and issubclass(value, BaseModel):
        return value.model_json_schema()
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    return str(value)


class _PromptCounter:
    """Walks a provider request and adds up its tokens. Any guess, such as
    media or text no tokenizer counted, marks the whole count estimated,
    which is what keeps the margin free."""

    def __init__(self, count_text: Optional[TextCounter], text_exact: bool):
        self._count_text = count_text
        self.estimated = count_text is None or not text_exact
        self.tokens = 0

    def text(self, value: str) -> None:
        if not value:
            return
        if self._count_text is None:
            self.tokens += estimate_text_tokens(value)
            return
        try:
            self.tokens += int(self._count_text(value))
        except Exception:
            self.estimated = True
            self.tokens += estimate_text_tokens(value)

    def estimate(self, tokens: int) -> None:
        self.estimated = True
        self.tokens += max(int(tokens), 0)

    def add_definition(self, value: Any) -> None:
        if value is None or (isinstance(value, (list, tuple, dict)) and not value):
            return
        self.estimated = True
        self.text(to_json_text(value))

    def add(self, value: Any, key: Optional[str] = None) -> None:
        if value is None or isinstance(value, bool):
            return
        if isinstance(value, BaseModel):
            value = value.model_dump(exclude_none=True)
        if isinstance(value, (bytes, bytearray)):
            self._payload(bytes(value), None)
        elif isinstance(value, str):
            if key in _MEDIA_KEYS and self._media_string(value, key):
                return
            self.text(value)
        elif isinstance(value, Mapping):
            self._mapping(value)
        elif isinstance(value, (list, tuple)):
            for item in value:
                self.add(item)
        elif isinstance(value, (int, float)):
            self.text(str(value))
        else:
            self.text(str(value))

    def _mapping(self, value: Mapping) -> None:
        block_type = value.get("type")
        if isinstance(block_type, str):
            if block_type in _IMAGE_BLOCK_TYPES:
                self.estimate(IMAGE_TOKENS_ESTIMATE)
                return
            if block_type in _FILE_BLOCK_TYPES:
                self._file(value)
                return
            if block_type in _AUDIO_BLOCK_TYPES:
                self.estimate(AUDIO_TOKENS_ESTIMATE)
                return

        for key, item in value.items():
            if key in _SKIPPED_KEYS:
                continue
            if key == "image" and isinstance(item, Mapping):
                # Bedrock Converse image block
                self.estimate(IMAGE_TOKENS_ESTIMATE)
            elif key == "video" and isinstance(item, Mapping):
                self.estimate(VIDEO_TOKENS_ESTIMATE)
            elif key == "document" and isinstance(item, Mapping):
                # Bedrock Converse document block
                self._file(item)
            elif key == "inline_data" and isinstance(item, Mapping):
                # Gemini inline media
                self._payload(item.get("data"), item.get("mime_type"))
            elif key == "file_data" and isinstance(item, Mapping):
                # Gemini media referenced by URI
                self.estimate(_estimate_for_mime(item.get("mime_type")))
            else:
                self.add(item, key=key)

    def _file(self, block: Mapping) -> None:
        payload, mime = _find_payload(block)
        if payload is None:
            self.estimate(FILE_TOKENS_ESTIMATE)
            return
        self._payload(payload, mime)

    def _media_string(self, value: str, key: str) -> bool:
        """Count a media payload held in a string. Returns False for text."""
        if value.startswith("data:") and ";base64," in value[:200]:
            self._payload(value, None)
            return True
        if key in _RAW_BASE64_KEYS and _looks_like_base64(value):
            self._payload(value, None)
            return True
        return False

    def _payload(self, payload: Any, mime: Optional[str]) -> None:
        """Estimate one image, file, audio or video payload."""
        if payload is None:
            self.estimate(FILE_TOKENS_ESTIMATE)
            return
        mime = (mime or "").lower()
        data: Optional[bytes] = None
        if isinstance(payload, (bytes, bytearray)):
            data = bytes(payload)
        elif isinstance(payload, str):
            if payload.startswith("data:"):
                header, _, encoded = payload.partition(",")
                mime = mime or header[5:].split(";")[0].lower()
                data = _decode_base64(encoded)
            elif mime.startswith("text/"):
                self.estimated = True
                self.text(payload)
                return
            else:
                data = _decode_base64(payload) if _looks_like_base64(payload) else None
                if data is None:
                    self.estimated = True
                    self.text(payload)
                    return
        if data is None:
            self.estimate(_estimate_for_mime(mime))
            return
        # marks the count an estimate; _bytes adds the tokens
        self.estimate(0)
        self._bytes(data, mime)

    def _bytes(self, data: bytes, mime: str) -> None:
        if mime.endswith("pdf") or data.startswith(b"%PDF"):
            self.tokens += _pdf_pages(data) * PDF_PAGE_TOKENS_ESTIMATE
        elif mime.startswith("image") or sniff_image_mime(data):
            self.tokens += IMAGE_TOKENS_ESTIMATE
        elif mime.startswith("video") or sniff_video_mime(data):
            self.tokens += VIDEO_TOKENS_ESTIMATE
        elif mime.startswith("audio"):
            self.tokens += AUDIO_TOKENS_ESTIMATE
        else:
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                self.tokens += math.ceil(len(data) / BINARY_BYTES_PER_TOKEN_ESTIMATE)
                return
            self.text(text)


def _find_payload(block: Mapping) -> "tuple[Optional[Any], Optional[str]]":
    """The first payload and mime hint inside a file block, at any depth."""
    payload = None
    mime = None
    stack = [block]
    while stack:
        current = stack.pop(0)
        for key in _MIME_KEYS:
            hint = current.get(key)
            if mime is None and isinstance(hint, str) and hint:
                mime = _mime_from_hint(key, hint) or None
        for key in _PAYLOAD_KEYS:
            item = current.get(key)
            if payload is None and isinstance(item, (str, bytes, bytearray)) and item:
                payload = item
        for item in current.values():
            if isinstance(item, Mapping):
                stack.append(item)
    return payload, mime


def _mime_from_hint(key: str, hint: str) -> str:
    hint = hint.lower()
    if key in ("filename", "name"):
        extension = hint.rsplit(".", 1)[-1] if "." in hint else ""
        if extension == "pdf":
            return "application/pdf"
        if extension in ("txt", "md", "csv", "html", "json", "xml"):
            return "text/" + extension
        return ""
    if key == "format":
        if hint == "pdf":
            return "application/pdf"
        if hint in ("txt", "md", "csv", "html"):
            return "text/" + hint
        return hint
    return hint


def _estimate_for_mime(mime: Optional[str]) -> int:
    mime = (mime or "").lower()
    if mime.startswith("image"):
        return IMAGE_TOKENS_ESTIMATE
    if mime.startswith("video"):
        return VIDEO_TOKENS_ESTIMATE
    if mime.startswith("audio"):
        return AUDIO_TOKENS_ESTIMATE
    return FILE_TOKENS_ESTIMATE


def _looks_like_base64(value: str) -> bool:
    # long, with no spaces and only base64 characters, so ordinary text under
    # a media key is still counted as text; only the start is checked, as
    # payloads can be megabytes
    return (
        len(value) >= 64
        and " " not in value[:256]
        and bool(_BASE64_RE.match(value[:4096]))
    )


def _decode_base64(value: str) -> Optional[bytes]:
    cleaned = value.strip().replace("\n", "").replace("\r", "")
    padded = cleaned + "=" * (-len(cleaned) % 4)
    try:
        return base64.b64decode(padded, validate=True)
    except (binascii.Error, ValueError):
        try:
            return base64.urlsafe_b64decode(padded)
        except (binascii.Error, ValueError):
            return None


def _pdf_pages(data: bytes) -> int:
    pages = len(_PDF_PAGE_RE.findall(data))
    if pages:
        return pages
    # page objects inside compressed object streams cannot be seen
    return max(1, math.ceil(len(data) / PDF_BYTES_PER_PAGE_ESTIMATE))
