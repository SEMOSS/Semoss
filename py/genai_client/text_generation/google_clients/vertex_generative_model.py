# https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/send-chat-prompts-gemini

from typing import Optional, List
from vertexai.preview.generative_models import GenerativeModel, Content, Part
from vertexai.generative_models._generative_models import gapic_content_types

from .abstract_vertex_textgen_client import AbstractVertextAiTextGeneration
from ...constants import AskModelEngineResponse, FULL_PROMPT


class VertexGenerativeModelClient(AbstractVertextAiTextGeneration):
    """
    Vertex AI class to use GenerativeModel and more specifically Gemini
    """

    def _get_client(self):
        return GenerativeModel(self.model_name)

    def ask_call(
        self,
        question: str = None,
        context: Optional[str] = None,
        history: Optional[List] = [],
        max_new_tokens: Optional[int] = 500,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        top_k: Optional[int] = None,
        candidate_count: Optional[int] = None,
        stop_sequences: Optional[List[str]] = None,
        prefix="",
        stream: Optional[bool] = True,
        **kwargs
    ):
        assert self.client != None
        
        if self.max_tokens != None:
            max_new_tokens = self.max_tokens

        chat = None
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

        # convert history to Content class
        historyChat = []
        if context != None:
            # There is no context with gemini models. This is mocking the context set up.
            historyChat.append(Content(role="user", parts=[Part.from_text(context)]))
            historyChat.append(
                Content(role="model", parts=[Part.from_text("Understood.")])
            )

        try:
            historyChat.extend(
                [
                    Content(
                        role=msg.get("author", msg["role"]),
                        parts=[Part.from_text(msg["content"])],
                    )
                    for msg in history
                ]
            )
        except KeyError:
            raise KeyError(
                "Unable to determine author of the message. No 'author' or 'role' provided."
            )

        # begin the convo
        chat = self.client.start_chat(history=historyChat)

        # convert ask inputs to vertex ai params
        generation_config = {
            "temperature": temperature,  # Temperature controls the degree of randomness in token selection.
            "max_output_tokens": max_new_tokens,  # Token limit determines the maximum amount of text output.
            "top_p": top_p,  # Tokens are selected from most probable to least until the sum of their probabilities equals the top_p value.
            "top_k": top_k,  # A top_k of 1 means the selected token is the most probable among all tokens.
            "stop_sequences": stop_sequences,
            "candidate_count": candidate_count,
        }

        # Initialize an empty dictionary to store the mapped safety settings
        mapped_safety_settings = {}
        # Iterate over each category and threshold in the safety settings
        for category, threshold in self.safety_settings.items():
            try:
                # Convert the category to an uppercase attribute of HarmCategory to match with the enum
                harm_category = getattr(gapic_content_types.HarmCategory, category.upper())
                # Convert the threshold to an uppercase attribute of HarmBlockThreshold to match with the enum
                harm_block_threshold = getattr(gapic_content_types.SafetySetting.HarmBlockThreshold, threshold.upper())
                mapped_safety_settings[harm_category] = harm_block_threshold
            except AttributeError as e:
                # Log the error if the category or threshold is invalid
                print(f"Invalid category or threshold: {category}, {threshold}. Error: {e}")
        
        if stream:
            responses = chat.send_message(
                content=question,
                generation_config=generation_config,
                stream=stream,
                safety_settings=mapped_safety_settings
            )

            model_engine_response = AskModelEngineResponse(response="")
            for response in responses:
                model_engine_response.response += response.text
                print(prefix + response.text, end="")

                usage_metadata = response._raw_response.usage_metadata
                if usage_metadata:
                    model_engine_response.prompt_tokens = (
                        usage_metadata.prompt_token_count
                    )
                    model_engine_response.response_tokens = (
                        usage_metadata.candidates_token_count
                    )

            return model_engine_response
        else:
            response = chat.send_message(
                content=question,
                generation_config=generation_config,
                stream=stream,
                safety_settings=mapped_safety_settings
            )

            model_engine_response = AskModelEngineResponse(
                response=response.text,
                prompt_tokens=response._raw_response.usage_metadata.prompt_token_count,
                response_tokens=response._raw_response.usage_metadata.candidates_token_count,
            )

            return model_engine_response
