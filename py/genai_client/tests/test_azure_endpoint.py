"""Azure endpoints, in each form a portal shows, reach the right url offline."""

import httpx
import pytest
from openai import NotFoundError

from genai_client.embedders.azure_openai_embedder import AzureOpenAiEmbedder
from genai_client.text_generation.openai_clients.openai_client import OpenAiClient


def recorder():
    urls = []

    def handle(request):
        urls.append(str(request.url))
        return httpx.Response(404, json={"error": {"code": "404"}})

    return urls, httpx.Client(transport=httpx.MockTransport(handle))


@pytest.mark.parametrize(
    "endpoint, api_version, url",
    [
        # Foundry's v1 url, or a route under it: no second /openai, no api-version
        (
            "https://x.services.ai.azure.com/openai/v1",
            "2024-11-20",
            "https://x.services.ai.azure.com/openai/v1/responses",
        ),
        (
            "https://x.services.ai.azure.com/openai/v1/responses/",
            "2024-11-20",
            "https://x.services.ai.azure.com/openai/v1/responses",
        ),
        # a resource url keeps the api-version path
        (
            "https://x.openai.azure.com/",
            "2025-04-01-preview",
            "https://x.openai.azure.com/openai/responses?api-version=2025-04-01-preview",
        ),
    ],
)
def test_text_client_azure_endpoint(endpoint, api_version, url):
    urls, http_client = recorder()
    client = OpenAiClient(
        is_azure=True,
        model_name="gpt-4o",
        api_key="fixture",
        endpoint=endpoint,
        api_version=api_version,
        chat_type="responses",
        http_client=http_client,
        max_retries=0,
    )
    with pytest.raises(NotFoundError):
        client.client.responses.create(model="gpt-4o", input="ping")
    assert urls == [url]


def test_embedder_azure_v1_endpoint(monkeypatch):
    # the tokenizer downloads its encoding, which an offline test cannot
    monkeypatch.setattr(AzureOpenAiEmbedder, "_get_tokenizer", lambda self, args: None)
    urls, http_client = recorder()
    embedder = AzureOpenAiEmbedder(
        model_name="text-embedding-3-small",
        api_key="fixture",
        endpoint="https://x.services.ai.azure.com/openai/v1/",
        api_version="2024-10-21",
        http_client=http_client,
        max_retries=0,
    )
    with pytest.raises(NotFoundError):
        embedder.client.embeddings.create(
            model="text-embedding-3-small", input=["ping"]
        )
    assert urls == ["https://x.services.ai.azure.com/openai/v1/embeddings"]
