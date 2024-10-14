from typing import List, Dict, Tuple

from .abstract_openai_client import AbstractOpenAiClient
from ...constants import FULL_PROMPT, IMAGE_ENCODED, AskModelEngineResponse


class OpenAiChatCompletion(AbstractOpenAiClient):
    def ask_call(
        self,
        question: str = None,
        context: str = None,
        template_name: str = None,
        history: List[Dict] = None,
        max_new_tokens=1000,
        prefix="",
        **kwargs,
    ) -> AskModelEngineResponse:
        if "repetition_penalty" in kwargs.keys():
            kwargs["frequency_penalty"] = float(kwargs.pop("repetition_penalty"))
        if "stop_sequences" in kwargs.keys():
            kwargs["stop"] = kwargs.pop("stop_sequences")

        if template_name == None:
            template_name = self.template_name

        operation = kwargs.pop("operation", None)
        if operation == "instruct":
            return self._do_instruct(
                question=question,
                context=context,
                max_new_tokens=max_new_tokens,
                prefix=prefix,
                **kwargs,
            )

        # first we determine the type of completion, since this determines how we
        # structure the payload
        # the list to construct the payload from
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

        # check to see if we need to adjust the prompt or max_new_tokens
        prompt, kwargs["max_tokens"], model_engine_response = self._check_token_limits(
            prompt_payload=message_payload, max_new_tokens=max_new_tokens
        )

        # add the message payload as a kwarg
        kwargs["messages"] = prompt

        model_engine_response.response = self._inference_call(prefix=prefix, **kwargs)
        model_engine_response.response_tokens = self.tokenizer.count_tokens(
            model_engine_response.response
        )

        return model_engine_response

    def _detect_task_target(
        self, question: str, context: str, max_new_tokens: int, prefix: str, **kwargs
    ):
        print("Detecting Task Target...")
        temp = kwargs.get("temperature", 0.1)
        top_p = kwargs.get("top_p", 0.2)
        max_tokens = max_new_tokens

        system_message = f"""Imagine you have a task {question}. Explain in a single sentence who the task should be intended for."""

        messages = []

        if context:
            messages.append({"role": "system", "content": context})

        messages.append({"role": "system", "content": system_message})

        messages.append({"role": "user", "content": ""})

        prompt_payload, adjusted_max_new_tokens, detect_task_response = (
            self._check_token_limits(prompt_payload=messages, max_new_tokens=max_tokens)
        )

        updated_kwargs = kwargs.copy()
        updated_kwargs.update(
            {
                "messages": prompt_payload,
                "temperature": temp,
                "top_p": top_p,
                "max_tokens": adjusted_max_new_tokens,
                "stream": False,
            }
        )

        response = self._inference_call(prefix=prefix, **updated_kwargs)

        task_target = response.strip()

        return task_target, detect_task_response

    def _decompose_task(
        self,
        question: str,
        task_target: str,
        context: str,
        max_new_tokens,
        prefix: str,
        **kwargs,
    ):
        print("Decomposing Task...")
        temp = kwargs.get("temperature", 0.1)
        top_p = kwargs.get("top_p", 0.2)
        max_tokens = max_new_tokens
        system_message = (
            f"{task_target}\n"
            + """
                ### Context:
                When faced with a large, complex task we need to employ a systematic approach to break it down into more manageable sub-tasks. This process involves analyzing the task, understanding its components for each sub-task. Throughout the decomposition process, iteratively define the methodology for each sub-task, which includes concrete instructions or algorithms specific to that sub-task. Each step or instruction within the methodology should be atomic, clear, and actionable. Based on the defined methodology evaluate the size and complexity of each sub-task and further divide it into smaller steps if necessary. This process is recursive until all subtasks, inputs, outputs, constraints, and Methodologies are thoroughly defined. Then proceed with task execution utilizing the output from previous steps as input for subsequent tasks.

                ### Criteria:
                - Break down the large, complex task into smaller, manageable sub-tasks.
                - Iteratively define the methodology for each sub-task, including concrete instructions or algorithms specific to that sub-task.
                - Ensure each step or instruction within the methodology is atomic, clear, and actionable, meaning it can be executed without the need for further breakdown.
                - Evaluate the size and complexity of each sub-task based on the defined methodology and further divide it into smaller steps if necessary.
                - Ensure all sub-tasks, inputs, outputs, constraints, and methodologies are thoroughly defined before proceeding with task execution.
                - Present the complete sub-task structure, including well-defined input, output, methodology, and possibly constraints for each sub-task.
                - Utilize the output from completed sub-tasks as input for subsequent tasks.
                - Ensure the successful completion of the entire task by effectively managing the task decomposition.

                Use the format: {"Steps": [{"Title": "title", "Action": "detailed action to perform", "Input Data": "input data items", "Output Data": "output data items", "Methodology":"defined methodology for the action","Assessment": "action assessment"}]}
                """
        )

        messages = []

        if context:
            messages.append({"role": "system", "content": context})

        messages.append({"role": "system", "content": system_message})

        user_message = f"### Task:\n{question}\n### Response:"
        messages.append({"role": "user", "content": user_message})

        prompt_payload, adjusted_max_new_tokens, decompose_response = (
            self._check_token_limits(prompt_payload=messages, max_new_tokens=max_tokens)
        )

        updated_kwargs = kwargs.copy()
        updated_kwargs.update(
            {
                "messages": prompt_payload,
                "temperature": temp,
                "top_p": top_p,
                "max_tokens": adjusted_max_new_tokens,
                "stream": False,
            }
        )

        response = self._inference_call(prefix=prefix, **updated_kwargs)

        start_index = response.find("{")
        end_index = response.rfind("}")
        if start_index != -1 and end_index != -1:
            output_data = response[start_index : end_index + 1]
        else:
            output_data = response.strip()

        return output_data, decompose_response

    def _do_instruct(
        self,
        question: str,
        context: str,
        max_new_tokens: int,
        prefix: str,
        **kwargs,
    ) -> AskModelEngineResponse:
        """
        Handles the 'instruct' operation.
        """
        print("Handling Instructions")

        task_target, detect_task_response = self._detect_task_target(
            question, context, max_new_tokens, prefix, **kwargs
        )

        final_data, decompose_response = self._decompose_task(
            question, task_target, context, max_new_tokens, prefix, **kwargs
        )

        final_response = AskModelEngineResponse()
        final_response.response = final_data
        final_response.prompt_tokens = (
            detect_task_response.prompt_tokens + decompose_response.prompt_tokens
        )
        final_response.response_tokens = self.tokenizer.count_tokens(final_data)
        warnings = [detect_task_response.warning, decompose_response.warning]
        final_response.warning = "\n\n".join(filter(None, warnings))

        return final_response

    def _inference_call(self, prefix: str, **kwargs) -> str:
        final_query = ""

        kwargs["stream"] = kwargs.get("stream", True)
        openai_response = self.client.chat.completions.create(
            model=self.model_name, **kwargs
        )

        if kwargs["stream"]:
            for chunk in openai_response:
                if chunk.choices and (len(chunk.choices) > 0):
                    response = chunk.choices[0].delta.content
                    if response != None:
                        final_query += response
                        print(prefix + response, end="")
        else:
            if "function_call" in kwargs.keys():
                final_query = openai_response.choices[0].message.function_call.arguments
            else:
                final_query = openai_response.choices[0].message.content

        return final_query

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
        if context is not None and template_name == None:
            if isinstance(context, str):
                context = self.fill_context(context, **mapping)[0]
                message_payload.append({"role": "system", "content": context})
        elif context != None and template_name != None:
            mapping.update({"context": context})
            context = self.fill_template(template_name=template_name, **mapping)[0]
            message_payload.append({"role": "system", "content": context})
        else:
            if template_name != None:
                possibleContent = self.fill_template(
                    template_name=template_name, **mapping
                )[0]
                if possibleContent != None:
                    message_payload.append(
                        {"role": "system", "content": possibleContent}
                    )

        # if history was added, then add it to the payload. Currently history is being like OpenAI prompts
        if history is not None:
            message_payload.extend(history)

        # check if images are in the fill args
        if IMAGE_ENCODED in fill_variables:
            # add the new question to the payload
            if question != None and len(question) > 0:
                image_payload = []
                image_payload.append({"type": "text", "text": question})
                image_url = {}
                image_url["url"] = (
                    f"data:image/png;base64,{fill_variables.pop(IMAGE_ENCODED)}"
                )
                image_payload.append({"type": "image_url", "image_url": image_url})
                message_payload.append({"role": "user", "content": image_payload})
        else:
            # add the new question to the payload
            if question != None and len(question) > 0:
                message_payload.append({"role": "user", "content": question})

        return message_payload

    def _process_full_prompt(self, full_prompt: List) -> List[Dict]:
        if isinstance(full_prompt, list):
            listOfDicts = set([isinstance(x, dict) for x in full_prompt]) == {True}
            if listOfDicts == False:
                raise ValueError("The provided payload is not valid")

            # now we have to check the key value pairs are valid
            all_keys_set = {key for d in full_prompt for key in d.keys()}
            validOpenAiDictKey = sorted(all_keys_set) == ["content", "role"]
            if validOpenAiDictKey == False:
                raise ValueError("There are invalid OpenAI dictionary keys")
            # add it the message payload
            return full_prompt
        else:
            raise TypeError(
                "Please make sure the full prompt for OpenAI Chat-Completion is a list"
            )

    def _check_token_limits(
        self, prompt_payload: List, max_new_tokens: int
    ) -> Tuple[str, int, AskModelEngineResponse]:
        """
        The method is used to truncate the the number of tokens in the prompt and adjust the `max_new_tokens` so that the text generation does not fail.
        Instead we rather will send back a flag indicating adjustments have
        """
        model_engine_response = AskModelEngineResponse()
        warnings = []

        specific_tokenizer = self.tokenizer._get_tokenizer(self.model_name)
        if hasattr(specific_tokenizer, "apply_chat_template"):
            # Apply the chat template to the prompt if no chat template was provided
            if specific_tokenizer.chat_template == None:
                specific_tokenizer.chat_template = "chatml"
            # there is a apply chat template available for this model - transformers tokenizer
            prompt = specific_tokenizer.apply_chat_template(
                prompt_payload, tokenize=False
            )
            # use the models tokenizer to get the number of tokens in the prompt
            prompt_tokens = self.tokenizer.get_tokens_ids(prompt)
            num_token_in_prompt = len(prompt_tokens)
        else:
            # use the models tokenizer to get the number of tokens in the prompt
            # this is likely directly openai
            num_token_in_prompt = self.tokenizer.count_tokens(prompt_payload)

        max_prompt_tokens = self.tokenizer.get_max_input_token_length()

        if max_prompt_tokens != None:
            max_tokens = max_prompt_tokens
        else:
            max_tokens = self.tokenizer.get_max_token_length()

        # perform the checks using max_tokens
        if num_token_in_prompt > max_tokens:
            token_counter = 0
            for i, message in enumerate(prompt_payload):
                num_message_tokens = self.tokenizer.count_tokens(message)
                token_counter += num_message_tokens

                if token_counter > max_tokens:
                    # calculate how many tokens we can take from this message
                    num_tokens_to_remove = token_counter - max_tokens

                    message_tokens = self.tokenizer.get_tokens(message["content"])
                    message_tokens = message_tokens[
                        : len(message_tokens) - num_tokens_to_remove
                    ]

                    prompt_payload[i]["content"] = "".join(message_tokens)
                    prompt_payload = prompt_payload[: i + 1]
                    warnings.append(f"The prompt was truncated to:\n {prompt_payload}")
                    num_token_in_prompt = self.tokenizer.count_tokens(prompt_payload)
                    break

        # now we also need to make sure the max_new_tokens passed in is adjusted
        if max_new_tokens > (max_tokens - num_token_in_prompt):
            max_new_tokens = max_new_tokens + (
                (max_tokens - num_token_in_prompt) - max_new_tokens
            )
            warnings.append(f"max_new_tokens was changed to: {max_new_tokens}")

        model_engine_response.prompt_tokens = num_token_in_prompt
        if len(warnings) > 0:
            model_engine_response.warning = "\\n\\n".join(warnings)

        return prompt_payload, int(max_new_tokens), model_engine_response
