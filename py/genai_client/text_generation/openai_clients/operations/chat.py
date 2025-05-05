from typing import List, Dict
from ....constants import (
    FULL_PROMPT,
    IMAGE_ENCODED,
    IMAGE_URL,
    AskModelEngineResponse,
)
from ....model_limits import ModelLimits


class Chat:
    def __init__(self, client):
        self.client = client

    def ask(
        self,
        question: str = None,
        context: str = None,
        template_name: str = None,
        use_history: bool = True,  # To control history tracking
        history: List[Dict] = None,
        # We should now expect max_completion_tokens but I can't get rid of this yet..
        # max_new_tokens=None,  # Deprecated # We dont use either of these?? I think they are passed in the kwargs
        # max_completion_tokens=None,  # We dont use either of these?? I think they are passed in the kwargs
        prefix="",
        **kwargs,
    ) -> AskModelEngineResponse:
        kwargs = self._normalize_kwargs(kwargs)

        if template_name is None:
            template_name = self.client.template_name

        # first we determine the type of completion, since this determines how we structure the payload
        message_payload = []

        if FULL_PROMPT not in kwargs:
            message_payload = self._process_chat_completion(
                question=question,
                context=context,
                history=(
                    history if use_history else None
                ),  # Only include history if use_history is True
                template_name=template_name,
                fill_variables=kwargs,
            )
        else:
            message_payload = self._process_full_prompt(kwargs.pop(FULL_PROMPT))

        model_limits = ModelLimits(
            model_name=self.client.model_name,
            context_window_smss=self.client.tokenizer.context_window,
            max_tokens_call_param=kwargs.pop("max_tokens", None),
            max_completion_tokens_call_param=kwargs.pop("max_completion_tokens", None),
            max_tokens_smss=self.client.tokenizer.max_tokens,
            max_completion_tokens_smss=self.client.tokenizer.max_completion_tokens,
        )

        max_tokens = model_limits.max_completion_tokens
        context_window = model_limits.context_window

        # Check to see if we need to truncate the prompt or adjust max_completion_tokens
        prompt, kwargs["max_tokens"], model_engine_response = (
            self.client.check_token_limits(
                messages=message_payload,
                max_tokens=max_tokens,
                context_window=context_window,
            )
        )

        # Add the message payload as a kwargs
        kwargs["messages"] = prompt

        (
            model_engine_response.response,
            model_engine_response.response_tokens,
            model_engine_response.messageType,
        ) = self.client.inference_call(prefix=prefix, **kwargs)

        if model_engine_response.response_tokens is None:
            model_engine_response.response_tokens = self.client.tokenizer.count_tokens(
                model_engine_response.response
            )

        return model_engine_response

    def _normalize_kwargs(self, kwargs: Dict) -> Dict:
        if "repetition_penalty" in kwargs:
            kwargs["frequency_penalty"] = float(kwargs.pop("repetition_penalty"))
        if "stop_sequences" in kwargs:
            kwargs["stop"] = kwargs.pop("stop_sequences")
        return kwargs

    def _process_chat_completion(
        self,
        question: str,
        context: str,
        history: List[Dict],
        template_name: str,
        fill_variables: Dict,
    ) -> List[Dict]:
        # the list to construct the payload from
        message_payload = []

        # if the user provided context, use that. Otherwise, try to get it from the template
        mapping = {"question": question} | fill_variables
        if context is not None and template_name is None:
            if isinstance(context, str):
                context = self.client.fill_context(context, **mapping)[0]
                message_payload.append({"role": "system", "content": context})
        elif context is not None and template_name is not None:
            mapping.update({"context": context})
            context = self.client.fill_template(template_name=template_name, **mapping)[
                0
            ]
            message_payload.append({"role": "system", "content": context})
        else:
            if template_name is not None:
                possibleContent = self.client.fill_template(
                    template_name=template_name, **mapping
                )[0]
                if possibleContent is not None:
                    message_payload.append(
                        {"role": "system", "content": possibleContent}
                    )

        # if history was added, then add it to the payload. Currently history is being like OpenAI prompts
        if history is not None:
            message_payload.extend(history)

        # check if images are in the fill args
        if IMAGE_ENCODED in fill_variables or IMAGE_URL in fill_variables:
            # add the new question to the payload
            if question:
                image_payload = [{"type": "text", "text": question}]
                image_url = {
                    "url": (
                        f"data:image/png;base64,{fill_variables.pop(IMAGE_ENCODED)}"
                        if IMAGE_ENCODED in fill_variables
                        else fill_variables.pop(IMAGE_URL)
                    )
                }
                image_payload.append({"type": "image_url", "image_url": image_url})
                message_payload.append({"role": "user", "content": image_payload})
        else:
            # add the new question to the payload
            if question:
                message_payload.append({"role": "user", "content": question})

        return message_payload

    def _process_full_prompt(self, full_prompt: List) -> List[Dict]:
        if isinstance(full_prompt, list):
            listOfDicts = set([isinstance(x, dict) for x in full_prompt]) == {True}
            if not listOfDicts:
                raise ValueError("The provided payload is not valid")

            # now we have to check the key value pairs are valid
            all_keys_set = {key for d in full_prompt for key in d.keys()}
            validOpenAiDictKey = sorted(all_keys_set) == ["content", "role"]
            if not validOpenAiDictKey:
                raise ValueError("There are invalid OpenAI dictionary keys")
            # add it the message payload
            return full_prompt
        else:
            raise TypeError(
                "Please make sure the full prompt for OpenAI Chat-Completion is a list"
            )
