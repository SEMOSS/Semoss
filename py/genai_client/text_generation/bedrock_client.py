import boto3
import botocore.exceptions
import logging
import re
import base64
import urllib.request
from .abstract_text_generation_client import AbstractTextGenerationClient
from ..tokenizers.huggingface_tokenizer import HuggingfaceTokenizer
from ..constants import (
    MAX_TOKENS,
    MAX_INPUT_TOKENS,
    FULL_PROMPT,
    IMAGE_ENCODED,
    IMAGE_URL,
    IMAGE_EXTENSION,
    AskModelEngineResponse,
)
from .bedrock_clients.bedrock_client2 import BedrockClient2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BedrockClient(AbstractTextGenerationClient):

    def __init__(
        self,
        template=None,
        service_name="bedrock-runtime",
        modelId="anthropic.claude-instant-v1",
        access_key=None,
        secret_key=None,
        region=None,
        template_name=None,
        response_stream=None,
        guardrail_identifier=None,
        guardrail_version=None,
        **kwargs,
    ):
        super().__init__(template=template, template_name=template_name)
        self.kwargs = kwargs
        self.modelId = modelId
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service_name = service_name
        self.response_stream = response_stream
        self.guardrail_identifier = guardrail_identifier
        self.guardrail_version = guardrail_version

        # get tokenizer based on model
        tokenizer_name = self._get_default_tokenizer(modelId)
        self.tokenizer = HuggingfaceTokenizer(
            encoder_name=tokenizer_name,
            max_tokens=kwargs.pop(MAX_TOKENS, None),
            max_input_tokens=kwargs.pop(MAX_INPUT_TOKENS, None),
        )

    def _get_default_tokenizer(self, modelId):
        """Retrieve the default tokenizer for a given model."""
        model_tokenizer_map = {
            "anthropic.claude-instant-v1": "bert-base-uncased",
        }
        return model_tokenizer_map.get(modelId, "bert-base-uncased")

    def _get_client(self):
        if self.access_key and self.secret_key:
            return boto3.client(
                service_name=self.service_name,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
            )
        else:
            return boto3.client(
                # assuming this is environment auth
                service_name=self.service_name,
                region_name=self.region,
            )

    def create_inference_config(self, max_new_tokens, temperature, top_p):
        if top_p is None:
            top_p = 0.9

        if temperature is None:
            temperature = 0.9

        # Base inference parameters to use.
        inference_config = {
            "temperature": temperature,
            "maxTokens": max_new_tokens,
            "topP": top_p,
        }

        return inference_config

    def _prepare_message_payload(
        self, question, context, template_name, history, use_history, **kwargs
    ):
        """Prepare the message payload for the Bedrock API request."""
        if FULL_PROMPT in kwargs:
            return [
                self._format_message_content(
                    {"role": "user", "content": kwargs[FULL_PROMPT]}
                )
            ]

        message_payload = []
        mapping = {"question": question} | kwargs

        # TODO context here should not be user but system...anthropic doesnt like system prompts tho.
        # if context:
        #     content = self._handle_context(context, template_name, **mapping)
        #     message_payload.append(
        #         self._format_message_content({"role": "system", "content": content})
        #     )
        # elif template_name:
        #     content = self.fill_template(template_name=template_name, **mapping)[0]
        #     if content:
        #         message_payload.append(
        #             self._format_message_content({"role": "system", "content": content})
        #         )

        if use_history and history is not None:
            message_payload.extend(
                [self._format_message_content(msg) for msg in history]
            )

        if question:
            message_payload.append(
                self._format_message_content({"role": "user", "content": question})
            )

        return message_payload

    def _handle_context(self, context, template_name, **mapping):
        """Handle context processing based on template."""
        if template_name:
            mapping.update({"context": context})
            return self.fill_template(template_name=template_name, **mapping)[0]
        elif isinstance(context, str):
            return self.fill_context(context, **mapping)[0]
        return context

    def _create_request_params(
        self,
        messages,
        inference_config,
        guardrail_config=None,
        system_prompt=None,
        tools=None,
        tool_choice=None,
    ):
        """Create the request parameters for the Bedrock API."""
        params = {
            "modelId": self.modelId,
            "messages": messages,
            "inferenceConfig": inference_config,
        }

        if guardrail_config:
            params["guardrailConfig"] = guardrail_config
        if system_prompt:
            params["system"] = system_prompt
        if tools is not None:
            params["tools"] = tools
        if tool_choice is not None:
            params["toolChoice"] = tool_choice
        return params

    def _handle_stream_response(self, prefix: str, stream_response):
        """Process streaming response from Bedrock."""
        final_response = ""
        for event in stream_response:
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
        return final_response, prompt_tokens, output_tokens

    def _get_guardrail_config(self):
        """Create guardrail configuration if enabled."""
        if self.guardrail_identifier and self.guardrail_version:
            return {
                "guardrailIdentifier": self.guardrail_identifier,
                "guardrailVersion": self.guardrail_version,
                "trace": "enabled",
            }
        return None

    def _get_raw_content(self, message_payload):
        """Extract raw content from messages before formatting."""
        return " ".join(msg["content"] for msg in message_payload)

    def _format_message_content(self, message):
        """Format a single message to match Bedrocks expected structure."""
        return {"role": message["role"], "content": message["content"]}

    def _format_messages_for_model(self, message_payload, full_prompt=None):
        """Format messages according to model and API requirements."""
        if self.modelId == "anthropic.claude-instant-v1":
            if full_prompt:
                # formatted_text = f"\n\nHuman:{full_prompt}\n\nAssistant:"
                return self.decode_image_bytes_in_messages(full_prompt)
            else:
                formatted_parts = []
                for msg in message_payload:
                    role = "Human" if msg["role"] in ["user", "system"] else "Assistant"
                    formatted_parts.append(f"\n\n{role}: {msg['content']}")
                formatted_text = "".join(formatted_parts) + "\n\nAssistant:"

            return [{"role": "user", "content": [{"text": formatted_text}]}]
        else:
            if full_prompt:
                # return [{"role": "user", "content": [{"text": full_prompt}]}]
                return self.decode_image_bytes_in_messages(full_prompt)
            else:
                return [
                    {"role": msg["role"], "content": [{"text": msg["content"]}]}
                    for msg in message_payload
                ]

    def _get_image_extension_from_url(self, image_url):
        """Get the image extension from image url."""
        if (
            "jpg" in image_url
            or "jpeg" in image_url
            or "JPG" in image_url
            or "JPEG" in image_url
        ):
            return "jpeg"
        elif "png" in image_url or "PNG" in image_url:
            return "png"
        elif "gif" in image_url or "GIF" in image_url:
            return "gif"
        elif "webp" in image_url or "WEBP" in image_url:
            return "webp"
        else:
            raise ValueError(
                "Invalid Image Extension - Expected 'jpeg', 'png', 'gif' or 'webp'"
            )

    def _get_bytes_from_encoded(self, base64_str):
        """Convert the encoded base64 string to raw bytes."""
        try:
            return base64.b64decode(base64_str)
        except Exception as e:
            logger.error(f"Failed to get bytes from encoded image - {e}")

    def _get_bytes_from_url(self, image_url):
        """Convert the bytes of image."""
        try:
            with urllib.request.urlopen(image_url) as response:
                image_data = response.read()

            return image_data
        except Exception as e:
            logger.error(f"Failed to get bytes from image - {e}")

    def _handle_image_params(self, question: str, kwargs: dict, message_payload):
        """
        Handle image parameters in the payload.
        """
        message_payload = []
        image_payload = [{"text": question}]

        key_to_pop = IMAGE_ENCODED if IMAGE_ENCODED in kwargs else IMAGE_URL
        images = kwargs.pop(key_to_pop)
        if isinstance(images, str):
            if key_to_pop == IMAGE_ENCODED:
                image_extension = IMAGE_EXTENSION
                image_bytes = self._get_bytes_from_encoded(images)
            else:
                image_extension = self._get_image_extension_from_url(images)
                image_bytes = self._get_bytes_from_url(images)
            image_url = {
                "format": image_extension,
                "source": {"bytes": image_bytes},
            }
            image_payload.append({"image": image_url})
            message_payload.append({"role": "user", "content": image_payload})
            return message_payload
        elif isinstance(images, list):
            for image in images:
                if key_to_pop == IMAGE_ENCODED:
                    image_extension = IMAGE_EXTENSION
                    image_bytes = self._get_bytes_from_encoded(image)
                else:
                    image_extension = self._get_image_extension_from_url(image)
                    image_bytes = self._get_bytes_from_url(image)
                image_url = {
                    "format": image_extension,
                    "source": {"bytes": image_bytes},
                }
                image_payload.append({"image": image_url})
            message_payload.append({"role": "user", "content": image_payload})
            return message_payload
        else:
            raise ValueError(
                f"Invalid type for {key_to_pop}. Expected str or list, got {type(images)}"
            )

    def ask_call(
        self,
        question=None,
        context=None,
        template_name=None,
        history=None,
        max_new_tokens=500,
        temperature=None,
        top_p=None,
        stop_sequences=None,
        prefix="",
        stream=True,
        use_history=True,  # To control history
        **kwargs,
    ) -> AskModelEngineResponse:
        try:
            client = self._get_client()
            model_engine_response = AskModelEngineResponse()

            if "message_json" in kwargs:
                bedrock_client2 = BedrockClient2(
                    cfg_client=self,
                    modelId=self.modelId,
                    access_key=self.access_key,
                    secret_key=self.secret_key,
                    region=self.region,
                    **kwargs,
                )
                return bedrock_client2.ask_call(
                    question=question,
                    context=context,
                    use_history=use_history,
                    history=history,
                    prefix=prefix,
                    **kwargs,
                )

            message_payload = self._prepare_message_payload(
                question, context, template_name, history, use_history, **kwargs
            )
            system_prompt = (
                [{"text": context}]
                if context is not None and isinstance(context, str)
                else None
            )

            raw_content = kwargs.get(FULL_PROMPT) or self._get_raw_content(
                message_payload
            )
            # TODO - THIS CAN BE REMOVED NOW?
            model_engine_response.prompt_tokens = self.tokenizer.count_tokens(
                raw_content
            )

            messages = self._format_messages_for_model(
                message_payload, kwargs.get(FULL_PROMPT)
            )

            inference_config = self.create_inference_config(
                max_new_tokens, temperature, top_p
            )

            guardrail_config = self._get_guardrail_config()

            should_stream = stream if stream is not None else self.response_stream
            should_stream = should_stream in (True, "true")

            if IMAGE_ENCODED in kwargs or IMAGE_URL in kwargs:
                messages = self._handle_image_params(question, kwargs, message_payload)

            request_params = self._create_request_params(
                messages, inference_config, guardrail_config, system_prompt
            )

            ### Detect tools/tool_choice
            tool_choice = kwargs.get("tool_choice", None)
            tool_config = kwargs.get("tools", None)
            if tool_config is not None:
                if not (
                    isinstance(tool_config, dict)
                    and "tools" in tool_config
                    and isinstance(tool_config["tools"], list)
                ):
                    raise ValueError(
                        'Expected "tools" to be a dict with key "tools" mapping to a list of toolSpecs. '
                        'Example: {"tools": [ ... ]}'
                    )
                # Remove any extra fields from user input - only allow "tools" and "toolChoice"
                new_tool_config = {
                    "tools": tool_config["tools"],
                }
                # Add toolChoice
                tc = tool_config.get("toolChoice")
                # Prefer explicit kwarg if present
                if tool_choice is not None:
                    if isinstance(tool_choice, dict):
                        new_tool_config["toolChoice"] = tool_choice
                    elif isinstance(tool_choice, str):
                        new_tool_config["toolChoice"] = {tool_choice: {}}
                    else:
                        raise ValueError("tool_choice must be a dict or string")
                elif tc is not None:
                    if isinstance(tc, dict):
                        new_tool_config["toolChoice"] = tc
                    else:
                        raise ValueError('"toolChoice" in tools dict must be a dict')
                else:
                    new_tool_config["toolChoice"] = {"auto": {}}
                request_params["toolConfig"] = new_tool_config
                should_stream = False

            if should_stream:
                try:
                    response = client.converse_stream(**request_params)
                except botocore.exceptions.ParamValidationError as e:
                    logger.info(f"Param Validation Error Occurred: {e}")
                    messages[0]["content"][0]["text"] = str(
                        messages[0]["content"][0]["text"]
                    )  # convert to valid format
                    response = client.converse_stream(**request_params)

                (
                    final_response,
                    model_engine_response.prompt_tokens,
                    model_engine_response.response_tokens,
                ) = self._handle_stream_response(prefix, response.get("stream", []))
                model_engine_response.response_tokens = self.tokenizer.count_tokens(
                    final_response
                )
                model_engine_response.messageType = "CHAT"
            else:
                response = client.converse(**request_params)
                # -- Handle tool use and regular completion. Note multiple responses can be had in messages
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
                    final_response = "\n".join(
                        texts
                    )  # appending for new if multiple texts are returned
                    model_engine_response.messageType = "CHAT"

                model_engine_response.response_tokens = response["usage"][
                    "outputTokens"
                ]

            model_engine_response.response = final_response
            return model_engine_response

        except Exception as e:
            logger.error(f"Error while making request to Bedrock: {e}")
            raise

    def is_base64(self, s):
        if not isinstance(s, str) or len(s) == 0:
            return False
        s_clean = s.strip().replace("\n", "").replace(" ", "")
        if len(s_clean) % 4 != 0:
            return False
        if not re.fullmatch(r"[A-Za-z0-9+/]*={0,2}", s_clean):
            return False
        try:
            base64.b64decode(s_clean, validate=True)
            return True
        except Exception as e:
            logger.error(
                f"Base64 decode failed: {e} — Value: {s[:40]}..."
            )  # log first 40 chars
            return False

    def decode_image_bytes_in_messages(self, messages):
        if isinstance(messages, dict):
            messages = [messages]
        for msg in messages:
            if isinstance(msg, dict) and msg.get("role") == "user":
                content = msg.get("content", [])
                for part in content:
                    if isinstance(part, dict) and "image" in part:
                        image_block = part["image"]
                        if (
                            isinstance(image_block, dict)
                            and "source" in image_block
                            and isinstance(image_block["source"], dict)
                        ):
                            src = image_block["source"]
                            if "bytes" in src and isinstance(src["bytes"], str):
                                if self.is_base64(src["bytes"]):
                                    src["bytes"] = base64.b64decode(src["bytes"])
        return messages
