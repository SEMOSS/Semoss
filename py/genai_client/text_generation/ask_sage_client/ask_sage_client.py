import os, json, re
from typing import Any, Dict, List, Optional
from asksageclient import AskSageClient
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...message_builders.ask_sage_builder.ask_sage_message_builder import (
    AskSageMessageBuilder,
)
from ...constants import AskModelEngineResponse


_GEN_IMAGES_RE = re.compile(
    r"<gen-images>\s*(\{.*?\})\s*</gen-images>",
    flags=re.DOTALL | re.IGNORECASE,
)


class AskSage(AbstractTextGenerationClient):
    def __init__(
        self, api_key: str, email: str, user_url: str, server_url: str, **kwargs
    ):
        os.environ["REQUESTS_CA_BUNDLE"] = (
            "/usr/local/share/ca-certificates/custom/ca_bundle.crt"
        )
        os.environ["SSL_CERT_FILE"] = (
            "/usr/local/share/ca-certificates/custom/ca_bundle.crt"
        )

        super().__init__(**kwargs)

        self.client = AskSageClient(
            email=email,
            api_key=api_key,
            user_base_url=user_url,
            server_base_url=server_url,
        )

        self.message_builder = AskSageMessageBuilder(self.model_settings)

    def ask_call(self, prefix: str = "", **kwargs) -> AskModelEngineResponse:
        semoss_messages = self.build_semoss_messages(
            model_settings=self.model_settings, **kwargs
        )

        ask_sage_request, media_content = self.message_builder.build_request(
            semoss_messages
        )

        request_dict = ask_sage_request.model_dump(exclude_none=True)
        # streaming not supported by this package
        request_dict.pop("streaming", None)

        response = None

        try:
            if media_content:
                response = self.client.query_with_file(
                    file=media_content[0], **request_dict
                )
            else:
                response = self.client.query(**request_dict)

            if response.get("tool_calls"):
                return self.parse_tool_calls(response, request_dict)

            try:
                input_tokens = (
                    self.client.tokenizer(request_dict.get("message")).get("response")
                    or 0
                )
            except Exception:
                input_tokens = 0

            try:
                output_tokens = self.client.tokenizer(response).get("response") or 0
            except Exception:
                output_tokens = 0

            response_message = response.get("message", None)
            if not response_message:
                raise ValueError("No message found in AskSage response.")

            image_urls = self.extract_gen_images_urls(response_message)

            if not image_urls:
                return AskModelEngineResponse(
                    response=response_message,
                    response_tokens=output_tokens,
                    prompt_tokens=input_tokens,
                    messageType="CHAT",
                )
            else:
                return AskModelEngineResponse(
                    response=image_urls,
                    response_tokens=output_tokens,
                    prompt_tokens=input_tokens,
                    messageType="RESPONSE_MEDIA",
                )

        finally:
            if media_content:
                for path in media_content:
                    try:
                        if os.path.exists(path):
                            os.remove(path)
                    except Exception as e:
                        pass

    def parse_tool_calls(
        self, response: Dict[str, Any], request_dict: Dict[str, Any]
    ) -> AskModelEngineResponse:
        tool_call_results = []
        tool_calls = response.get("tool_calls", [])

        for tool_call in tool_calls:
            try:
                arguments = json.loads(
                    tool_call.get("function", {}).get("arguments", "{}")
                )
            except json.decoder.JSONDecodeError:
                arguments = tool_call.get("function", {}).get("arguments", {})

            tool_call_results.append(
                {
                    "id": tool_call.get("id", "N/A"),
                    "type": tool_call.get("type", "N/A"),
                    "name": tool_call.get("function", {}).get("name", "N/A"),
                    "arguments": arguments,
                }
            )
        try:
            input_tokens = (
                self.client.tokenizer(request_dict.get("message", None)).get(
                    "response", None
                )
                or 0
            )
        except:
            input_tokens = 0

        try:
            output_tokens = self.client.tokenizer(tool_calls).get("response", None) or 0
        except:
            output_tokens = 0

        return AskModelEngineResponse(
            response=tool_call_results,
            prompt_tokens=input_tokens,
            response_tokens=output_tokens,
            messageType="TOOL",
        )

    def extract_gen_images_urls(self, message: str) -> Optional[List[str]]:
        """
        If `message` contains a <gen-images>{...}</gen-images> block, parse it and
        return images.urls (list[str]) when present. Otherwise return None.
        """
        if not message:
            return None

        m = _GEN_IMAGES_RE.search(message)
        if not m:
            return None

        json_blob = m.group(1).strip()

        try:
            payload = json.loads(json_blob)
        except json.JSONDecodeError:
            return None

        images = payload.get("images") if isinstance(payload, dict) else None
        urls = images.get("urls") if isinstance(images, dict) else None

        if isinstance(urls, list) and all(isinstance(u, str) for u in urls) and urls:
            return urls

        return None
