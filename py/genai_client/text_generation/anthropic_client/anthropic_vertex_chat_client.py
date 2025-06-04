from typing import Optional, List
from anthropic import AnthropicVertex
from .abstract_anthropic_client import AbstractAnthropicClient
from ...constants import AskModelEngineResponse, FULL_PROMPT


class AnthropicVertexClient(AbstractAnthropicClient):

    def _get_client(self):
        return AnthropicVertex(project_id=self.project, region=self.region)

    def ask_call(
        self,
        question: str = None,
        context: Optional[str] = None,
        history: Optional[List] = None,
        max_new_tokens: Optional[int] = 500,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        top_k: Optional[int] = None,
        stop_sequences: Optional[List[str]] = None,
        use_history: Optional[bool] = True,
        prefix="",
        **kwargs
    ) -> AskModelEngineResponse:

        assert self.client != None

        # the list to construct the payload from
        message_payload = []

        if FULL_PROMPT in kwargs.keys():
            full_prompt = kwargs.pop(FULL_PROMPT)

            # make sure the full prompt param is an even list
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

        # Conditionally control the history
        history = history if use_history else []

        if history:
            message_payload.extend(history)

        # add the new question to the payload
        if question:
            message_payload.append({"role": "user", "content": question})

        responses = self.client.messages.create(
            model=self.model_name, messages=message_payload, max_tokens=self.max_tokens
        )

        final_response = ""
        for response in responses:
            final_response += response.text
            print(prefix + response.text, end="")

        model_engine_response = AskModelEngineResponse(response=final_response)

        return model_engine_response
