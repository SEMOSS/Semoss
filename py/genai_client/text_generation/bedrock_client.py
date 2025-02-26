import boto3
import logging

from .abstract_text_generation_client import AbstractTextGenerationClient
from ..tokenizers.huggingface_tokenizer import HuggingfaceTokenizer
from ..constants import (
    MAX_TOKENS,
    MAX_INPUT_TOKENS,
    FULL_PROMPT,
    AskModelEngineResponse,
)

# from langchain_core.prompts import PromptTemplate
# from langchain_community.document_loaders.csv_loader import CSVLoader
# from langchain_text_splitters import CharacterTextSplitter

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

        # hard code the tokenizer for now
        self.tokenizer = HuggingfaceTokenizer(
            encoder_name="bert-base-uncased",
            max_tokens=kwargs.pop(MAX_TOKENS, None),
            max_input_tokens=kwargs.pop(MAX_INPUT_TOKENS, None),
        )

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
        self, question, context, template_name, history, **kwargs
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

#TODO context here should not be user but system...anthropic doesnt like system prompts tho.
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

        if history is not None:
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

    def _create_request_params(self, messages, inference_config, guardrail_config=None, system_prompt=None):
        """Create the request parameters for the Bedrock API."""
        params = {
            "modelId": self.modelId,
            "messages": messages,
            "inferenceConfig": inference_config,
        }

        if guardrail_config:
            params["guardrailConfig"] = guardrail_config
        if system_prompt:
            params["system"]=system_prompt
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
        return final_response

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
                formatted_text = f"\n\nHuman:{full_prompt}\n\nAssistant:"
            else:
                formatted_parts = []
                for msg in message_payload:
                    role = "Human" if msg["role"] in ["user", "system"] else "Assistant"
                    formatted_parts.append(f"\n\n{role}: {msg['content']}")
                formatted_text = "".join(formatted_parts) + "\n\nAssistant:"

            return [{"role": "user", "content": [{"text": formatted_text}]}]
        else:
            if full_prompt:
                return [{"role": "user", "content": [{"text": full_prompt}]}]
            else:
                return [
                    {"role": msg["role"], "content": [{"text": msg["content"]}]}
                    for msg in message_payload
                ]

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
        **kwargs,
    ) -> AskModelEngineResponse:
        try:
            client = self._get_client()
            model_engine_response = AskModelEngineResponse()

            message_payload = self._prepare_message_payload(
                question, context, template_name, history, **kwargs
            )
            system_prompt = None
            if context is not None and isinstance(context, str):
                system_prompt = [{'text':context}]

            if kwargs.get(FULL_PROMPT):
                raw_content = kwargs[FULL_PROMPT]
            else:
                raw_content = self._get_raw_content(message_payload)

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

            if should_stream:
                response = client.converse_stream(
                    **self._create_request_params(
                        messages, inference_config, guardrail_config, system_prompt
                    )
                )
                final_response = self._handle_stream_response(prefix,
                    response.get("stream", [])
                )
                model_engine_response.response_tokens = self.tokenizer.count_tokens(
                    final_response
                )
            else:
                response = client.converse(
                    **self._create_request_params(
                        messages, inference_config, guardrail_config, system_prompt
                    )
                )
                output_message = response["output"]["message"]["content"]
                final_response = output_message[0]["text"] if output_message else ""
                model_engine_response.response_tokens = response["usage"][
                    "outputTokens"
                ]

            model_engine_response.response = final_response
            return model_engine_response

        except Exception as e:
            logger.error(f"Error while making request to Bedrock: {e}")
            raise
