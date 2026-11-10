"""Offline checks of fitting calls inside the usage ceiling; no model calls."""

import base64
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError
from google.genai import types

from genai_client.constants import (
    AskModelEngineResponse2,
    EmbeddingsModelEngineResponse2,
)
from genai_client.embedders.abstract_embedder import AbstractEmbedder
from genai_client.message_builders.anthropic.anthropic_models import (
    AnthropicRequestConfig,
)
from genai_client.message_builders.semoss_base.semoss_models import ModelSettings
from genai_client.text_generation.abstract_text_generation_client import (
    AbstractTextGenerationClient,
)
from genai_client.text_generation.anthropic_client.anthropic_text_client import (
    AnthropicTextClient,
)
from genai_client.text_generation.bedrock_clients.bedrock_client import BedrockClient
from genai_client.text_generation.google_genai_clients.google_genai_client import (
    GoogleGenAiTextClient,
)
from genai_client.text_generation.model_engine_exception import (
    CONTEXT_OVERFLOW,
    ModelEngineException,
)
from genai_client.text_generation.openai_clients.openai_client import OpenAiClient
from genai_client.usage_ceiling import (
    IMAGE_TOKENS_ESTIMATE,
    LARGEST_OUTPUT_MAXIMUM,
    DOES_NOT_FIT_CODE,
    DOES_NOT_FIT_MESSAGE,
    PDF_PAGE_TOKENS_ESTIMATE,
    REQUEST_OVERHEAD_TOKENS,
    USAGE_CEILING,
    USAGE_LIMIT_DOES_NOT_FIT,
    PromptCount,
    UsageCeiling,
    UsageLimitDoesNotFitError,
    count_prompt_tokens,
    fit_thinking_budget,
    note_output_stop,
    usage_ceiling_scope,
)


def words(text):
    return len(text.split())


def exact(tokens):
    return PromptCount(tokens=tokens, estimated=False)


def estimated(tokens):
    return PromptCount(tokens=tokens, estimated=True)


RATES = {
    "input": 2.5,
    "output": 10.0,
    "cache_read": 1.25,
    "cache_write": None,
    "reasoning": None,
}


# ---------------------------------------------------------------------------
# Reading the keyword argument
# ---------------------------------------------------------------------------


def test_no_ceiling_reads_as_none():
    assert UsageCeiling.from_value(None) is None
    assert UsageCeiling.from_value("") is None
    assert UsageCeiling.from_value({"tokens": None, "cost_micros": None}) is None


def test_ceiling_reads_from_dict_and_json():
    from_dict = UsageCeiling.from_value({"tokens": 1000, "min_output_tokens": None})
    from_json = UsageCeiling.from_value(
        json.dumps({"cost_micros": 500, "rates": RATES})
    )
    assert from_dict.tokens == 1000
    assert from_dict.min_output_tokens == 256
    assert from_dict.margin == 0.05
    assert from_json.cost_micros == 500
    assert from_json.rates.output == 10.0


def test_ceiling_of_another_type_is_refused():
    with pytest.raises(TypeError):
        UsageCeiling.from_value(42)


# ---------------------------------------------------------------------------
# What is left for output
# ---------------------------------------------------------------------------


def test_tokens_left_for_output_are_the_ceiling_minus_the_prompt():
    assert UsageCeiling(tokens=10_000).output_left(exact(4_000)) == 6_000


def test_estimated_prompt_keeps_the_margin_free():
    # 10,000 less 5% is 9,500
    assert UsageCeiling(tokens=10_000).output_left(estimated(4_000)) == 5_500


def test_cost_left_for_output_prices_the_prompt_at_the_input_rate():
    ceiling = UsageCeiling(cost_micros=2_500_000, rates=RATES)
    # 100,000 prompt tokens at $2.50 per million cost 250,000 micros;
    # the 2,250,000 left buy 225,000 output tokens at $10 per million
    assert ceiling.output_left(exact(100_000)) == 225_000


def test_cost_left_for_output_uses_the_larger_of_output_and_reasoning():
    ceiling = UsageCeiling(cost_micros=2_500_000, rates={**RATES, "reasoning": 15.0})
    assert ceiling.output_left(exact(100_000)) == 150_000


def test_output_left_is_the_lesser_of_tokens_and_cost():
    ceiling = UsageCeiling(tokens=50_000, cost_micros=2_500_000, rates=RATES)
    assert ceiling.output_left(exact(10_000)) == 40_000
    ceiling = UsageCeiling(tokens=500_000, cost_micros=2_500_000, rates=RATES)
    assert ceiling.output_left(exact(100_000)) == 225_000


def test_prompt_costing_more_than_the_ceiling_leaves_nothing_for_output():
    ceiling = UsageCeiling(cost_micros=1_000, rates=RATES)
    assert ceiling.output_left(exact(1_000)) == 0


def test_free_output_is_not_limited_by_cost():
    ceiling = UsageCeiling(cost_micros=1_000_000, rates={"input": 1.0, "output": 0.0})
    assert ceiling.output_left(exact(1_000)) is None


# ---------------------------------------------------------------------------
# Refusal and the cap
# ---------------------------------------------------------------------------


def test_refused_when_output_left_is_under_min_output_tokens():
    ceiling = UsageCeiling(tokens=1_000)
    with pytest.raises(UsageLimitDoesNotFitError) as refused:
        ceiling.fit_output(exact(800), requested_max=4_000)
    assert refused.value.output_left == 200
    assert refused.value.needed == 256
    assert str(refused.value).startswith(DOES_NOT_FIT_MESSAGE)
    assert "conversation history" in str(refused.value)


def test_cap_lowers_the_callers_maximum_to_what_is_left():
    fit = UsageCeiling(tokens=5_000).fit_output(exact(1_000), requested_max=8_000)
    assert fit.max_tokens == 4_000
    assert fit.lowered


def test_cap_never_raises_the_callers_maximum():
    fit = UsageCeiling(tokens=5_000).fit_output(exact(1_000), requested_max=2_000)
    assert fit.max_tokens == 2_000
    assert not fit.lowered


def test_cap_sets_a_maximum_when_the_caller_has_none():
    fit = UsageCeiling(tokens=5_000).fit_output(exact(1_000), requested_max=None)
    assert fit.max_tokens == 4_000
    assert fit.lowered


def test_output_left_beyond_any_model_maximum_sets_none():
    ceiling = UsageCeiling(tokens=LARGEST_OUTPUT_MAXIMUM + 2_000)
    fit = ceiling.fit_output(exact(1_000), requested_max=None)
    assert fit.max_tokens is None
    assert not fit.lowered


def test_embeddings_input_must_fit():
    assert UsageCeiling(tokens=1_000).input_fits(exact(1_000))
    assert not UsageCeiling(tokens=1_000).input_fits(estimated(1_000))
    assert not UsageCeiling(cost_micros=100, rates={"input": 2.0}).input_fits(
        exact(100)
    )


def test_embeddings_are_priced_at_the_input_rate():
    # embeddings are never cached, so a cache write rate does not apply
    ceiling = UsageCeiling(cost_micros=200, rates={"input": 2.0, "cache_write": 4.0})
    assert ceiling.input_fits(exact(100))


# ---------------------------------------------------------------------------
# The output maximum the call is reserved for
# ---------------------------------------------------------------------------


def test_ceiling_reads_the_output_maximum():
    assert UsageCeiling.from_value({"tokens": 1_000}).max_output_tokens is None
    ceiling = UsageCeiling.from_value({"tokens": 1_000, "max_output_tokens": 8_192})
    assert ceiling.max_output_tokens == 8_192


def test_no_caller_maximum_gets_the_reserved_maximum_when_more_is_left():
    # what is left is a whole day's limit; a provider would refuse it as max_tokens
    ceiling = UsageCeiling(tokens=90_000, max_output_tokens=8_192)
    fit = ceiling.fit_output(exact(1_000), requested_max=None)
    assert fit.max_tokens == 8_192
    assert fit.lowered


def test_no_caller_maximum_gets_what_is_left_when_it_is_smaller():
    ceiling = UsageCeiling(tokens=5_000, max_output_tokens=8_192)
    fit = ceiling.fit_output(exact(1_000), requested_max=None)
    assert fit.max_tokens == 4_000
    assert fit.lowered


def test_the_reserved_maximum_never_raises_the_callers():
    ceiling = UsageCeiling(tokens=90_000, max_output_tokens=8_192)
    fit = ceiling.fit_output(exact(1_000), requested_max=2_000)
    assert fit.max_tokens == 2_000
    assert not fit.lowered


def test_no_rule_on_the_output_sets_no_maximum():
    ceiling = UsageCeiling(
        cost_micros=1_000_000,
        rates={"input": 1.0, "output": 0.0},
        max_output_tokens=8_192,
    )
    fit = ceiling.fit_output(exact(1_000), requested_max=None)
    assert fit.max_tokens is None
    assert not fit.lowered


def test_an_answer_stopped_at_the_reserved_maximum_is_not_marked_capped():
    larger = UsageCeiling(tokens=90_000, max_output_tokens=8_192)
    with usage_ceiling_scope(larger) as ceiling_call:
        ceiling_call.fit_output(exact(1_000), requested_max=None)
        assert not ceiling_call.capped(8_192)
    smaller = UsageCeiling(tokens=5_000, max_output_tokens=8_192)
    with usage_ceiling_scope(smaller) as ceiling_call:
        ceiling_call.fit_output(exact(1_000), requested_max=None)
        assert ceiling_call.capped(4_000)


# ---------------------------------------------------------------------------
# Cache writes
# ---------------------------------------------------------------------------


def test_cost_left_for_output_prices_the_prompt_at_a_higher_cache_write_rate():
    ceiling = UsageCeiling(cost_micros=2_500_000, rates={**RATES, "cache_write": 3.125})
    # 100,000 prompt tokens at $3.125 per million cost 312,500 micros; the
    # 2,187,500 left buy 218,750 output tokens at $10 per million
    assert ceiling.output_left(exact(100_000)) == 218_750


def test_a_cache_write_rate_below_the_input_rate_leaves_the_input_rate():
    ceiling = UsageCeiling(cost_micros=2_500_000, rates={**RATES, "cache_write": 1.0})
    assert ceiling.output_left(exact(100_000)) == 225_000


# ---------------------------------------------------------------------------
# Thinking budgets
# ---------------------------------------------------------------------------


def test_thinking_budget_that_fits_is_kept():
    assert fit_thinking_budget(2_000, 10_000, 256, 1024) == 2_000


def test_thinking_budget_is_lowered_leaving_part_for_the_answer():
    assert fit_thinking_budget(8_000, 5_000, 256, 1024) == 4_744


def test_thinking_turns_off_when_the_smallest_budget_does_not_fit():
    assert fit_thinking_budget(8_000, 1_000, 256, 1024) is None


def test_thinking_that_cannot_turn_off_keeps_its_smallest_budget():
    assert fit_thinking_budget(4_096, 300, 256, 128, can_turn_off=False) == 128


# ---------------------------------------------------------------------------
# Counting a prompt
# ---------------------------------------------------------------------------


def test_text_without_a_tokenizer_is_estimated():
    count = count_prompt_tokens(["abc" * 100])
    assert count == estimated(100 + REQUEST_OVERHEAD_TOKENS)


def test_text_with_a_tokenizer_is_exact():
    count = count_prompt_tokens(["a b c"], count_text=words)
    assert count == exact(3 + REQUEST_OVERHEAD_TOKENS)


def test_images_are_estimated():
    image = {
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": "image/png",
            "data": "iVBORw0K" * 20,
        },
    }
    count = count_prompt_tokens([[image]], count_text=words)
    assert count == estimated(IMAGE_TOKENS_ESTIMATE + REQUEST_OVERHEAD_TOKENS)


def test_pdf_documents_are_estimated_by_page():
    pdf = b"%PDF-1.4\n<< /Type /Page >>\n<< /Type /Page >>\n<< /Type /Pages >>"
    document = {"document": {"format": "pdf", "name": "doc", "source": {"bytes": pdf}}}
    count = count_prompt_tokens([[document]], count_text=words)
    assert count == estimated(2 * PDF_PAGE_TOKENS_ESTIMATE + REQUEST_OVERHEAD_TOKENS)


def test_text_files_are_counted_as_text():
    data = base64.b64encode(b"hello world").decode()
    block = {
        "type": "file",
        "file": {"file_data": f"data:text/plain;base64,{data}", "filename": "a.txt"},
    }
    count = count_prompt_tokens([[block]], count_text=words)
    assert count == estimated(2 + REQUEST_OVERHEAD_TOKENS)


def test_replayed_signatures_are_not_counted():
    block = {"type": "thinking", "thinking": "abc", "signature": "x" * 3_000}
    count = count_prompt_tokens([[block]], count_text=words)
    assert count == exact(2 + REQUEST_OVERHEAD_TOKENS)


def test_tool_definitions_make_the_count_an_estimate():
    count = count_prompt_tokens(["hi"], [[{"name": "lookup"}]], count_text=words)
    assert count.estimated


# ---------------------------------------------------------------------------
# The error response
# ---------------------------------------------------------------------------


def test_does_not_fit_error_details_carry_the_reason():
    error = UsageLimitDoesNotFitError(estimated(5_000), output_left=100, needed=256)
    details = ModelEngineException(
        error=error, client="openai", model="gpt-4o"
    ).parse_error()
    assert details.messageType == "ERROR"
    assert details.reason == USAGE_LIMIT_DOES_NOT_FIT
    assert details.reason != CONTEXT_OVERFLOW
    assert details.code == DOES_NOT_FIT_CODE
    assert details.message.startswith(DOES_NOT_FIT_MESSAGE)
    assert "prompt_tokens=5000" in details.reason_detail


# ---------------------------------------------------------------------------
# AbstractTextGenerationClient.ask
# ---------------------------------------------------------------------------


class FakeClient(AbstractTextGenerationClient):
    """Fits a fixed prompt to the ceiling and answers with a chosen length."""

    def __init__(
        self, prompt_tokens=100, requested=4_000, response_tokens=None, stop_at_max=None
    ):
        self.model_name = "fake-model"
        self.prompt_tokens = prompt_tokens
        self.requested = requested
        self.response_tokens = response_tokens
        self.stop_at_max = stop_at_max
        self.seen_kwargs = None
        self.fit = None

    def ask_call(self, *args, **kwargs):
        self.seen_kwargs = kwargs
        self.fit = self.fit_output_to_usage_ceiling(
            self.requested, lambda: exact(self.prompt_tokens)
        )
        max_tokens = self.fit.max_tokens if self.fit else self.requested
        if self.stop_at_max is not None:
            note_output_stop(self.stop_at_max)
        tokens = (
            self.response_tokens if self.response_tokens is not None else max_tokens
        )
        return AskModelEngineResponse2(
            response="answer", prompt_tokens=self.prompt_tokens, response_tokens=tokens
        )


def test_ask_never_passes_the_ceiling_to_the_client():
    client = FakeClient()
    client.ask(message_json="[]", usage_ceiling={"tokens": 10_000})
    assert USAGE_CEILING not in client.seen_kwargs
    assert client.seen_kwargs == {"message_json": "[]"}


def test_ask_without_a_ceiling_fits_nothing():
    client = FakeClient()
    response = client.ask()
    assert client.fit is None
    assert "usageCapped" not in response


def test_ask_marks_a_response_that_used_the_lowered_maximum():
    client = FakeClient(prompt_tokens=100, requested=4_000)
    response = client.ask(usage_ceiling={"tokens": 1_000})
    assert client.fit.max_tokens == 900
    assert response["usageCapped"] is True


def test_ask_marks_a_response_the_provider_stopped_at_the_maximum():
    client = FakeClient(requested=4_000, response_tokens=850, stop_at_max=True)
    response = client.ask(usage_ceiling={"tokens": 1_000})
    assert response["usageCapped"] is True


def test_ask_does_not_mark_a_response_that_ended_early():
    client = FakeClient(requested=4_000, response_tokens=300, stop_at_max=False)
    response = client.ask(usage_ceiling={"tokens": 1_000})
    assert "usageCapped" not in response


def test_ask_does_not_mark_a_maximum_the_ceiling_left_alone():
    client = FakeClient(requested=500, response_tokens=500)
    response = client.ask(usage_ceiling={"tokens": 10_000})
    assert "usageCapped" not in response


def test_ask_refuses_with_the_does_not_fit_error():
    client = FakeClient(prompt_tokens=100)
    response = client.ask(usage_ceiling={"tokens": 300})
    assert response["messageType"] == "ERROR"
    assert response["reason"] == USAGE_LIMIT_DOES_NOT_FIT
    assert response["code"] == DOES_NOT_FIT_CODE
    assert response["message"].startswith(DOES_NOT_FIT_MESSAGE)


# ---------------------------------------------------------------------------
# AbstractEmbedder.embeddings
# ---------------------------------------------------------------------------


class FakeEmbedder(AbstractEmbedder):
    def __init__(self):
        self.model_name = "fake-embedder"
        self.tokenizer = None
        self.calls = []

    def _get_tokenizer(self, init_args):
        return None

    def embeddings_call(self, strings_to_embed, **kwargs):
        self.calls.append(kwargs)
        return EmbeddingsModelEngineResponse2(
            response=[[0.0] for _ in strings_to_embed], prompt_tokens=1
        )


def test_embeddings_refuse_an_input_that_does_not_fit():
    embedder = FakeEmbedder()
    # 300 bytes are estimated at 100 tokens, over 95% of 50
    response = embedder.embeddings(["x" * 300], usage_ceiling={"tokens": 50})
    assert embedder.calls == []
    assert response["messageType"] == "ERROR"
    assert response["reason"] == USAGE_LIMIT_DOES_NOT_FIT
    assert response["response"] == response["message"]
    assert "input uses about 100 tokens" in response["message"]


def test_embeddings_refuse_an_input_that_costs_too_much():
    embedder = FakeEmbedder()
    response = embedder.embeddings(
        ["x" * 300], usage_ceiling={"cost_micros": 100, "rates": {"input": 2.0}}
    )
    assert response["reason"] == USAGE_LIMIT_DOES_NOT_FIT


def test_embeddings_run_without_the_ceiling_when_the_input_fits():
    embedder = FakeEmbedder()
    response = embedder.embeddings(["hello"], usage_ceiling={"tokens": 1_000})
    assert embedder.calls == [{}]
    assert response["response"] == [[0.0]]


def test_multi_modal_media_counts_as_an_image_each():
    embedder = FakeEmbedder()
    count = embedder.count_multi_modal_embedding_tokens(["x" * 300], ["a"], ["b"])
    assert count == estimated(100 + 2 * IMAGE_TOKENS_ESTIMATE)
    assert embedder.count_multi_modal_embedding_tokens(["x" * 300]) == estimated(100)


def test_multi_modal_embeddings_refuse_an_input_that_does_not_fit():
    embedder = FakeEmbedder()
    refused = embedder.multi_modal_input_error({"tokens": 3_000}, ["hi"], ["image"])
    assert refused["reason"] == USAGE_LIMIT_DOES_NOT_FIT
    assert refused["response"] == refused["message"]
    fits = embedder.multi_modal_input_error({"tokens": 10_000}, ["hi"], ["image"])
    assert fits is None
    assert embedder.multi_modal_input_error(None, ["hi"], ["image"]) is None


# ---------------------------------------------------------------------------
# OpenAI and Azure OpenAI
# ---------------------------------------------------------------------------


def openai_client(chat_type="chat-completion"):
    client = OpenAiClient.__new__(OpenAiClient)
    client.model_settings = ModelSettings(model_name="gpt-4o")
    client.model_name = "gpt-4o"
    client.chat_type = chat_type
    client.is_azure = False
    client.endpoint = None
    client._usage_text_counter_resolved = (words, True)
    return client


def test_openai_chat_lowers_max_completion_tokens():
    client = openai_client()
    request = {
        "messages": [{"role": "user", "content": "hello world"}],
        "max_completion_tokens": 4_000,
    }
    # "user" and "hello world", one message and the request overhead
    assert client._count_prompt_tokens(request, "chat-completion") == exact(10)
    with usage_ceiling_scope(UsageCeiling(tokens=1_000)):
        client._apply_usage_ceiling(request, "chat-completion")
    assert request["max_completion_tokens"] == 990


def test_openai_chat_lowers_max_tokens_set_by_an_override():
    client = openai_client()
    request = {
        "messages": [{"role": "user", "content": "hello world"}],
        "max_tokens": 4_000,
    }
    with usage_ceiling_scope(UsageCeiling(tokens=1_000)):
        client._apply_usage_ceiling(request, "chat-completion")
    assert request["max_tokens"] == 990
    assert "max_completion_tokens" not in request


def test_openai_chat_sets_a_maximum_when_there_is_none():
    client = openai_client()
    request = {"messages": [{"role": "user", "content": "hello world"}]}
    with usage_ceiling_scope(UsageCeiling(tokens=1_000)):
        client._apply_usage_ceiling(request, "chat-completion")
    assert request["max_completion_tokens"] == 990


def test_openai_responses_lowers_max_output_tokens():
    client = openai_client("responses")
    request = {
        "instructions": "be brief",
        "input": [{"role": "user", "content": "hello world"}],
        "max_output_tokens": 4_000,
    }
    with usage_ceiling_scope(UsageCeiling(tokens=1_000)):
        client._apply_usage_ceiling(request, "responses")
    assert request["max_output_tokens"] == 988


def test_openai_images_keep_the_margin_free():
    client = openai_client()
    request = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "hello world"},
                    {
                        "type": "image_url",
                        "image_url": {"url": "https://example.com/a.png"},
                    },
                ],
            }
        ],
        "max_completion_tokens": 10_000,
    }
    prompt = client._count_prompt_tokens(request, "chat-completion")
    assert prompt.estimated
    with usage_ceiling_scope(UsageCeiling(tokens=10_000)):
        client._apply_usage_ceiling(request, "chat-completion")
    assert request["max_completion_tokens"] == 9_500 - prompt.tokens


def test_openai_refuses_before_calling_the_provider():
    client = openai_client()
    request = {"messages": [{"role": "user", "content": "hello world"}]}
    with usage_ceiling_scope(UsageCeiling(tokens=200)):
        with pytest.raises(UsageLimitDoesNotFitError):
            client._apply_usage_ceiling(request, "chat-completion")


def test_openai_responses_stop_at_max_output_tokens():
    stopped = SimpleNamespace(
        status="incomplete",
        incomplete_details=SimpleNamespace(reason="max_output_tokens"),
    )
    finished = SimpleNamespace(status="completed", incomplete_details=None)
    assert OpenAiClient._stopped_at_output_maximum(stopped)
    assert not OpenAiClient._stopped_at_output_maximum(finished)


# ---------------------------------------------------------------------------
# Anthropic
# ---------------------------------------------------------------------------


def anthropic_client(provider="anthropic", input_tokens=3_000):
    client = AnthropicTextClient.__new__(AnthropicTextClient)
    client.model_name = "claude-sonnet-4-5"
    client.provider = provider
    client.use_beta_header = False
    count_tokens = Mock(return_value=SimpleNamespace(input_tokens=input_tokens))
    client.client = SimpleNamespace(messages=SimpleNamespace(count_tokens=count_tokens))
    return client, count_tokens


def test_anthropic_batch_results_say_the_batch_has_ended():
    client, _ = anthropic_client()
    entry = SimpleNamespace(
        custom_id="r1",
        result=SimpleNamespace(
            type="succeeded",
            message=SimpleNamespace(
                role="assistant",
                content=[],
                usage=SimpleNamespace(input_tokens=3, output_tokens=4),
            ),
        ),
        model_dump=lambda: {"custom_id": "r1"},
    )
    batches = SimpleNamespace(results=lambda batch_id: [entry])
    client.client = SimpleNamespace(messages=SimpleNamespace(batches=batches))
    results = client.get_batch_results("msgbatch_1")
    # results can only be read once the batch has ended
    assert results["status"] == "COMPLETED"
    assert results["results"][0]["input_tokens"] == 3


def anthropic_request(thinking):
    return AnthropicRequestConfig(
        model="claude-sonnet-4-5",
        messages=[{"role": "user", "content": "hi"}],
        max_tokens=16_000,
        thinking=thinking,
    )


def test_anthropic_lowers_max_tokens_and_the_thinking_budget():
    client, count_tokens = anthropic_client()
    request = anthropic_request({"type": "enabled", "budget_tokens": 8_000})
    with usage_ceiling_scope(UsageCeiling(tokens=8_000)):
        client._apply_usage_ceiling(request)
    count_tokens.assert_called_once()
    assert request.max_tokens == 5_000
    assert request.thinking == {"type": "enabled", "budget_tokens": 4_744}


def test_anthropic_turns_thinking_off_when_1024_tokens_do_not_fit():
    client, _ = anthropic_client()
    request = anthropic_request({"type": "enabled", "budget_tokens": 8_000})
    with usage_ceiling_scope(UsageCeiling(tokens=4_000)):
        client._apply_usage_ceiling(request)
    assert request.max_tokens == 1_000
    assert request.thinking is None


def test_anthropic_leaves_adaptive_thinking_alone():
    client, _ = anthropic_client()
    request = anthropic_request({"type": "adaptive"})
    with usage_ceiling_scope(UsageCeiling(tokens=8_000)):
        client._apply_usage_ceiling(request)
    assert request.max_tokens == 5_000
    assert request.thinking == {"type": "adaptive"}


def test_anthropic_on_bedrock_estimates_the_prompt():
    client, count_tokens = anthropic_client(provider="bedrock")
    request = anthropic_request(None)
    with usage_ceiling_scope(UsageCeiling(tokens=8_000)):
        client._apply_usage_ceiling(request)
    count_tokens.assert_not_called()
    # "user" (2) and "hi" (1) estimated, one message, the request overhead;
    # 5% of 8,000 kept free
    assert request.max_tokens == 7_600 - 10


def test_anthropic_falls_back_to_an_estimate_when_counting_fails():
    client, count_tokens = anthropic_client()
    count_tokens.side_effect = RuntimeError("count failed")
    request = anthropic_request(None)
    with usage_ceiling_scope(UsageCeiling(tokens=8_000)):
        client._apply_usage_ceiling(request)
    assert request.max_tokens == 7_600 - 10


# ---------------------------------------------------------------------------
# Bedrock
# ---------------------------------------------------------------------------


def bedrock_client(count_tokens):
    client = BedrockClient.__new__(BedrockClient)
    client.model_name = "anthropic.claude-sonnet-4-5"
    client.model_id = "anthropic.claude-sonnet-4-5"
    client.has_schema = False
    client.client = SimpleNamespace(count_tokens=count_tokens)
    return client


def bedrock_request():
    return {
        "messages": [{"role": "user", "content": [{"text": "hi"}]}],
        "inferenceConfig": {"maxTokens": 10_000},
        "additionalModelRequestFields": {
            "reasoning_config": {"type": "enabled", "budget_tokens": 8_000}
        },
    }


def test_bedrock_prompt_tokens_include_cache_reads_and_writes():
    usage = {
        "inputTokens": 4,
        "outputTokens": 36,
        "totalTokens": 1_540,
        "cacheReadInputTokens": 1_480,
        "cacheWriteInputTokens": 20,
    }
    assert BedrockClient._normalize_usage(usage) == (1_504, 36, 1_480, 20, None)
    assert BedrockClient._normalize_usage({"inputTokens": 7, "outputTokens": 3}) == (
        7,
        3,
        None,
        None,
        None,
    )


def test_bedrock_response_reports_the_normalized_prompt():
    client = bedrock_client(Mock())
    client.client.converse = Mock(
        return_value={
            "output": {"message": {"content": [{"text": "hello"}]}},
            "stopReason": "end_turn",
            "usage": {
                "inputTokens": 4,
                "outputTokens": 36,
                "cacheReadInputTokens": 1_480,
                "cacheWriteInputTokens": 20,
            },
        }
    )
    response = client._handle_non_streaming({"messages": []})
    assert response.prompt_tokens == 1_504
    assert response.response_tokens == 36
    assert response.cache_read_tokens == 1_480
    assert response.cache_creation_tokens == 20


def test_bedrock_lowers_max_tokens_and_the_reasoning_budget():
    client = bedrock_client(Mock(return_value={"inputTokens": 2_000}))
    request = bedrock_request()
    with usage_ceiling_scope(UsageCeiling(tokens=6_000)):
        client._apply_usage_ceiling(request)
    assert request["inferenceConfig"]["maxTokens"] == 4_000
    assert (
        request["additionalModelRequestFields"]["reasoning_config"]["budget_tokens"]
        == 3_744
    )


def test_bedrock_turns_reasoning_off_when_1024_tokens_do_not_fit():
    client = bedrock_client(Mock(return_value={"inputTokens": 2_000}))
    request = bedrock_request()
    with usage_ceiling_scope(UsageCeiling(tokens=3_000)):
        client._apply_usage_ceiling(request)
    assert request["inferenceConfig"]["maxTokens"] == 1_000
    assert "reasoning_config" not in request["additionalModelRequestFields"]


def test_bedrock_estimates_when_count_tokens_is_unsupported():
    refused = ClientError(
        {"Error": {"Code": "ValidationException", "Message": "not supported"}},
        "CountTokens",
    )
    client = bedrock_client(Mock(side_effect=refused))
    request = bedrock_request()
    with usage_ceiling_scope(UsageCeiling(tokens=6_000)):
        client._apply_usage_ceiling(request)
    assert client._count_tokens_unavailable is True
    # "user" (2) and "hi" (1) estimated, one message, the request overhead;
    # 5% of 6,000 kept free
    assert request["inferenceConfig"]["maxTokens"] == 5_700 - 10


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------


def gemini_client(model_name, total_tokens=1_000):
    client = GoogleGenAiTextClient.__new__(GoogleGenAiTextClient)
    client.model_name = model_name
    count_tokens = Mock(return_value=SimpleNamespace(total_tokens=total_tokens))
    client.google_client = SimpleNamespace(
        models=SimpleNamespace(count_tokens=count_tokens)
    )
    return client


def gemini_config(budget=4_096):
    return types.GenerateContentConfig(
        max_output_tokens=8_192,
        thinking_config=types.ThinkingConfig(
            include_thoughts=True, thinking_budget=budget
        ),
    )


def gemini_contents():
    return [types.Content(role="user", parts=[types.Part.from_text(text="hi")])]


def test_gemini_lowers_max_output_tokens_and_the_thinking_budget():
    client = gemini_client("gemini-2.5-flash")
    config = gemini_config()
    with usage_ceiling_scope(UsageCeiling(tokens=3_000)):
        client._apply_usage_ceiling(gemini_contents(), config)
    assert config.max_output_tokens == 2_000
    assert config.thinking_config.thinking_budget == 1_744


def test_gemini_turns_thinking_off_when_the_smallest_budget_does_not_fit():
    client = gemini_client("gemini-2.5-flash-lite")
    config = gemini_config()
    with usage_ceiling_scope(UsageCeiling(tokens=1_600)):
        client._apply_usage_ceiling(gemini_contents(), config)
    assert config.max_output_tokens == 600
    assert config.thinking_config.thinking_budget == 0
    assert config.thinking_config.include_thoughts is False


def test_gemini_pro_keeps_its_smallest_thinking_budget():
    client = gemini_client("gemini-2.5-pro")
    config = gemini_config()
    with usage_ceiling_scope(UsageCeiling(tokens=1_300)):
        client._apply_usage_ceiling(gemini_contents(), config)
    assert config.max_output_tokens == 300
    assert config.thinking_config.thinking_budget == 128


def test_gemini_stops_at_max_tokens():
    stopped = SimpleNamespace(
        candidates=[SimpleNamespace(finish_reason=types.FinishReason.MAX_TOKENS)]
    )
    finished = SimpleNamespace(
        candidates=[SimpleNamespace(finish_reason=types.FinishReason.STOP)]
    )
    assert GoogleGenAiTextClient._stopped_at_output_maximum(stopped)
    assert not GoogleGenAiTextClient._stopped_at_output_maximum(finished)


# ---------------------------------------------------------------------------
# Older Vertex AI clients
# ---------------------------------------------------------------------------


def vertex_chat_client():
    pytest.importorskip("vertexai")
    from genai_client.text_generation.google_clients.vertext_chat_client import (
        VertexChatClient,
    )

    client = VertexChatClient.__new__(VertexChatClient)
    client.model_name = "chat-bison"
    return client


def test_vertex_clients_keep_their_maximum_without_a_ceiling():
    client = vertex_chat_client()
    assert client.fit_max_new_tokens(500, "context", [], "hello") == 500


def test_vertex_clients_lower_their_maximum_to_what_is_left():
    client = vertex_chat_client()
    with usage_ceiling_scope(UsageCeiling(tokens=1_000)):
        # "ctx" (1) and 300 bytes (100) estimated, one message and the request
        # overhead; 5% of 1,000 kept free
        assert client.fit_max_new_tokens(4_000, "ctx", [], "a" * 300) == 950 - 108


def test_vertex_clients_refuse_before_calling_the_provider():
    client = vertex_chat_client()
    with usage_ceiling_scope(UsageCeiling(tokens=300)):
        with pytest.raises(UsageLimitDoesNotFitError):
            client.fit_max_new_tokens(500, "ctx", [], "a" * 300)
