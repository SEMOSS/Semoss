# https://cloud.google.com/vertex-ai/generative-ai/docs/text/test-text-prompts#generative-ai-test-text-prompt-python_vertex_ai_sdk

from typing import Dict, Optional, List
from vertexai.language_models import TextGenerationModel

from .abstract_vertex_textgen_client import AbstractVertextAiTextGeneration
from ...constants import (
    AskModelEngineResponse,
    FULL_PROMPT
)


class VertexTextClient(AbstractVertextAiTextGeneration):

    def _get_client(
        self
    ):
        return TextGenerationModel.from_pretrained(self.model_name)

    def ask_call(
        self,
        question: str = None,
        context: Optional[str] = None,
        history: Optional[List] = [],
        max_new_tokens: Optional[int] = 500,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        top_k: Optional[int] = None,
        stop_sequences: Optional[List[str]] = None,
        prefix="",
        **kwargs
    ) -> AskModelEngineResponse:
        assert self.client != None

        chat = None
        if FULL_PROMPT in kwargs.keys():
            full_prompt = kwargs.pop(FULL_PROMPT)

            # make sure the full prompt param is an even list
            assert len(full_prompt) % 2 != 0

            # pull out the last message
            last_msg = full_prompt[-1]

            if isinstance(last_msg, dict):
                question = last_msg.get('content')
                history = full_prompt[:-1]
            elif isinstance(last_msg, str):
                question = last_msg
                history = []
            else:
                raise TypeError(
                    "Unable to extract the question from full prompt list")

        # build the message chain
        prompt = ''
        author = ''
        try:
            prompt += context or ''
            for msg in history:
                author = msg.get('author', msg['role']) + ':\n'
                content = msg['content']

                prompt += author
                prompt += content

                prompt += '\n\n'

            prompt += author
            prompt += question
        except KeyError:
            raise KeyError(
                "Unable to determine author of the message. No 'author' or 'role' provided.")

        # convert ask inputs to vertex ai params
        parameters = {
            "prompt": prompt,
            # Temperature controls the degree of randomness in token selection.
            "temperature": temperature,
            # Token limit determines the maximum amount of text output.
            "max_output_tokens": max_new_tokens,
            # Tokens are selected from most probable to least until the sum of their probabilities equals the top_p value.
            "top_p": top_p,
            # A top_k of 1 means the selected token is the most probable among all tokens.
            "top_k": top_k,
            "stop_sequences": stop_sequences,
        }

        responses = self.client.predict_streaming(
            **parameters
        )

        final_response = ''
        for response in responses:
            final_response += response.text

        model_engine_response = AskModelEngineResponse(response=final_response)

        return model_engine_response
