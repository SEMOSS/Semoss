from typing import Optional, List
from anthropic import AnthropicVertex
from .abstract_anthropic_client import AbstractAnthropicClient
from ...constants import AskModelEngineResponse, FULL_PROMPT


class AnthropicVertexClient(AbstractAnthropicClient):

    def _get_client(self):
        return AnthropicVertex(project_id=self.project, region=self.region)

    def _create_request_params(
        self, message_payload, temperature, top_p, stop_sequences, stream
    ):
        """Create the request parameters for the Anthropic Vertex API."""
        params = {
            "model": self.model_name,
            "messages": message_payload,
            "max_tokens": self.max_tokens,
            "temperature": temperature,
            "stop_sequences": stop_sequences,
            "stream": stream,
        }

        if top_p:
            params["top_p"] = top_p

        return params

    def _process_full_prompt(self, full_prompt, message_payload):
        """Extract question and history from FULL_PROMPT and create message payload."""
        # make sure the full prompt param is not an even list
        assert len(full_prompt) % 2 != 0

        # pull out the last message
        last_msg = full_prompt[-1]

        if isinstance(last_msg, dict):
            question = last_msg.get("content")
            history = full_prompt[:-1]
        elif isinstance(last_msg, str):
            question = last_msg
            history = []
        else:
            raise TypeError("Unable to extract the question from full prompt list")

        message_payload = history if history else []
        message_payload.append({"role": "user", "content": question})

        return message_payload

    def ask_call(
        self,
        question: str = None,
        context: Optional[str] = None,
        history: Optional[List] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        stop_sequences: Optional[List[str]] = None,
        use_history: Optional[bool] = True,
        stream: Optional[bool] = True,
        prefix="",
        **kwargs
    ) -> AskModelEngineResponse:

        assert self.client != None

        # the list to construct the payload from
        message_payload = []
        final_response = ""
        prompt_tokens = 0
        response_tokens = 0

        if FULL_PROMPT in kwargs:
            message_payload = self._process_full_prompt(
                full_prompt=kwargs.pop(FULL_PROMPT), message_payload=message_payload
            )

        # Conditionally control the history
        history = history if use_history else []

        if history:
            message_payload.extend(history)

        # add the new question to the payload
        if question:
            message_payload.append({"role": "user", "content": question})

        request_params = self._create_request_params(
            message_payload, temperature, top_p, stop_sequences, stream
        )

        responses = self.client.messages.create(**request_params)

        if stream:
            for response in responses:
                if "content_block_delta" in response.type:
                    content = response.delta.text
                    if content != None:
                        final_response += content
                        print(prefix + content, end="")
        else:
            final_response = responses.content[0].text
            prompt_tokens = responses.usage.input_tokens
            response_tokens = responses.usage.output_tokens

        model_engine_response = AskModelEngineResponse(
            response=final_response,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
        )

        return model_engine_response
