from typing import Any, Dict
import boto3
import botocore.exceptions
from ...message_builders.bedrock.bedrock_message_builder import BedrockMessageBuilder
from ...constants import AskModelEngineResponse


class BedrockClient2:
    def __init__(
        self,
        cfg_client,
        modelId: str,
        region: str,
        secret_key: str = None,
        access_key: str = None,
        **kwargs,
    ):
        self.cfg_client = cfg_client
        self.client = self._get_client(region, secret_key, access_key, **kwargs)
        self.model_id = modelId

    def _get_client(
        self, region: str, secret_key: str = None, access_key: str = None, **kwargs
    ) -> boto3.client:
        """
        Initialize the Bedrock client with credentials.
        """
        try:
            if access_key and secret_key:
                session = boto3.Session(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    region_name=region,
                )
            else:
                session = boto3.Session(region_name=region)

            return session.client("bedrock-runtime")

        except botocore.exceptions.BotoCoreError as e:
            raise RuntimeError(f"Failed to initialize Bedrock client: {e}")

    def ask_call(
        self,
        prefix="",
        **kwargs,
    ) -> AskModelEngineResponse:
        if self.client is None:
            raise RuntimeError("Bedrock client is not initialized.")

        self.ask_settings = self.cfg_client.get_ask_settings(
            self.cfg_client.model_settings, **kwargs
        )

        if self.ask_settings.semoss_messages:
            bedrock_request = BedrockMessageBuilder().build_messages(
                self.ask_settings.semoss_messages,
                system_prompt=self.ask_settings.system_prompt,
            )
        else:
            raise ValueError(
                "This class is only being used for message_json requests.."
            )

        streaming = bedrock_request.get("additionalModelRequestFields", None).pop(
            "stream", False
        )

        if self.ask_settings.streaming or streaming:
            return self._handle_streaming(bedrock_request, prefix=prefix)
        else:
            return self._handle_non_streaming(bedrock_request)

    def _handle_streaming(
        self, request: Dict[str, Any], prefix: str = ""
    ) -> AskModelEngineResponse:
        try:
            stream_response = self.client.converse_stream(
                modelId=self.model_id, **request
            )
            final_response = ""
            for event in stream_response.get("stream", []):
                if "contentBlockDelta" in event:
                    text = event["contentBlockDelta"]["delta"]["text"]
                    if text != None:
                        final_response += text
                        print(prefix + text, end="")
                if "metadata" in event:
                    metadata = event["metadata"]
                    if "usage" in metadata:
                        prompt_tokens = metadata["usage"]["inputTokens"]
                        output_tokens = metadata["usage"]["outputTokens"]
                    # Not aware of any scenarios where this would not be included
                    # Maybe we can remove instead of tokenizing ourselves..
                    else:
                        prompt_tokens = 0
                        output_tokens = 0
                else:
                    prompt_tokens = 0
                    output_tokens = 0
            return AskModelEngineResponse(
                response=final_response,
                prompt_tokens=prompt_tokens,
                response_tokens=output_tokens,
            )
        except botocore.exceptions.BotoCoreError as e:
            raise RuntimeError(f"Failed to stream response from Bedrock: {e}")

    def _handle_non_streaming(self, request: Dict[str, Any]) -> AskModelEngineResponse:
        model_engine_response = AskModelEngineResponse()
        try:
            response = self.client.converse(modelId=self.model_id, **request)

            output = response.get("output", {})
            message = output.get("message", {})
            content_list = message.get("content", [])
            tool_uses = []
            texts = []
            for content in content_list:
                if "toolUse" in content:
                    tool_use_block = content["toolUse"]
                    tool_uses.append(
                        {
                            "id": tool_use_block.get("toolUseId"),
                            "type": None,
                            "name": tool_use_block.get("name"),
                            "arguments": tool_use_block.get("input", {}),
                        }
                    )
                elif "text" in content:
                    texts.append(content["text"])
            if tool_uses:
                final_response = tool_uses
                model_engine_response.messageType = "TOOL"
            else:
                final_response = "\n".join(texts)
                model_engine_response.messageType = "CHAT"

            model_engine_response.response_tokens = response["usage"]["outputTokens"]
            model_engine_response.prompt_tokens = response["usage"]["inputTokens"]

            model_engine_response.response = final_response
            return model_engine_response
        except botocore.exceptions.BotoCoreError as e:
            raise RuntimeError(f"Failed to get response from Bedrock: {e}")
