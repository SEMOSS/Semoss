from typing import List, Dict
from ....constants import (
    FULL_PROMPT,
    IMAGE_ENCODED,
    IMAGE_URL,
    AskModelEngineResponse,
)


class Chat:
    def __init__(self, client):
        self.client = client

    def ask(
        self,
        question: str = None,
        context: str = None,
        template_name: str = None,
        history: List[Dict] = None,
        # We should now expect max_completion_tokens but I can't get rid of this yet..
        max_new_tokens=None,  # Deprecated
        max_completion_tokens=None,
        prefix="",
        **kwargs,
    ) -> AskModelEngineResponse:
        if "repetition_penalty" in kwargs.keys():
            kwargs["frequency_penalty"] = float(kwargs.pop("repetition_penalty"))
        if "stop_sequences" in kwargs.keys():
            kwargs["stop"] = kwargs.pop("stop_sequences")

        if template_name is None:
            template_name = self.client.template_name

        # first we determine the type of completion, since this determines how we structure the payload
        message_payload = []

        if FULL_PROMPT not in kwargs.keys():

            message_payload = self._process_chat_completion(
                question=question,
                context=context,
                history=history,
                template_name=template_name,
                fill_variables=kwargs,
            )

        else:
            message_payload = self._process_full_prompt(kwargs.pop(FULL_PROMPT))

        # We want to honor the new variable name first, then the old variable name but its okay if both are None
        # Once everyone is using the new variable name, we can remove this
        user_max_tokens = (
            max_completion_tokens
            if max_completion_tokens is not None
            else max_new_tokens
        )

        # Check to see if we need to truncate the prompt or adjust max_completion_tokens
        prompt, kwargs["max_tokens"], model_engine_response = (
            self.client.check_token_limits(
                prompt_payload=message_payload, user_max_tokens=user_max_tokens
            )
        )

        # Add the message payload as a kwarg
        kwargs["messages"] = prompt

        model_engine_response.response = self.client.inference_call(
            prefix=prefix, **kwargs
        )
        model_engine_response.response_tokens = self.client.tokenizer.count_tokens(
            model_engine_response.response
        )

        return model_engine_response

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
        if IMAGE_ENCODED in fill_variables:
            # add the new question to the payload
            if question is not None and len(question) > 0:
                image_payload = []
                image_payload.append({"type": "text", "text": question})
                image_url = {}
                image_url["url"] = (
                    f"data:image/png;base64,{fill_variables.pop(IMAGE_ENCODED)}"
                )
                image_payload.append({"type": "image_url", "image_url": image_url})
                message_payload.append({"role": "user", "content": image_payload})
        if IMAGE_URL in fill_variables:
            if question is not None and len(question) > 0:
                image_payload = []
                image_payload.append({"type": "text", "text": question})
                image_url = {}
                image_url["url"] = fill_variables.pop(IMAGE_URL)
                image_payload.append({"type": "image_url", "image_url": image_url})
                message_payload.append({"role": "user", "content": image_payload})
        else:
            # add the new question to the payload
            if question is not None and len(question) > 0:
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
