from ....constants import InstructModelEngineResponse


class Instruct:
    def __init__(self, client):
        self.client = client

    def instruct(
        self,
        task: str,
        context: str = None,
        max_new_tokens: int = 2000,
        prefix: str = "",
        **kwargs,
    ) -> InstructModelEngineResponse:
        """
        Handles the 'instruct' operation.
        """
        print("Executing Instruct Operation...")

        task_target, detect_task_response = self._detect_task_target(
            task, context, max_new_tokens, prefix, **kwargs
        )

        final_data, decompose_response = self._decompose_task(
            task, task_target, context, max_new_tokens, prefix, **kwargs
        )

        final_response = InstructModelEngineResponse()
        final_response.response = final_data
        final_response.prompt_tokens = decompose_response.prompt_tokens
        # final_response.response_tokens = self.client.tokenizer.count_tokens(
        #     decompose_response.response.decode("utf-8")
        # )
        final_response.response_tokens = 1
        warnings = [detect_task_response.warning, decompose_response.warning]
        final_response.warning = "\n\n".join(filter(None, warnings))

        return final_response

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
            self.client.check_token_limits(
                prompt_payload=messages, max_new_tokens=max_tokens
            )
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

        response = self.client.inference_call(prefix=prefix, **updated_kwargs)

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
            f"As an AI assistant, your task is to decompose the following task into a sequence of clear and actionable steps. "
            f"Please present the steps in JSON array format.\n\n"
            f"### Task Target:\n{task_target}\n\n"
            "### Instructions:\n"
            "- Break down the task into smaller, manageable steps.\n"
            "- Each step should be clear, concise, and actionable.\n"
            "- Do not include additional explanations or context.\n"
            "- Present the steps in JSON array format.\n"
            "- DO NOT RETURN ANYTHING OTHER THAN THE JSON ARRAY.\n\n"
            "### Example Output:\n"
            '["Description for step 1.", "Description for step 2.", "Description for step 3."]\n'
        )

        messages = []

        if context:
            messages.append({"role": "system", "content": context})

        messages.append({"role": "system", "content": system_message})

        user_message = f"### Task:\n{question}\n### Response:"
        messages.append({"role": "user", "content": user_message})

        prompt_payload, adjusted_max_new_tokens, decompose_response = (
            self.client.check_token_limits(
                prompt_payload=messages, max_new_tokens=max_tokens
            )
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

        response = self.client.inference_call(prefix=prefix, **updated_kwargs)

        parsed_response = self.parse_response(response)

        return parsed_response, decompose_response

    def parse_response(self, response):
        print("Parsing response... : ")
        import json

        try:
            steps = json.loads(response)
        except json.JSONDecodeError as e:
            print("Error parsing response with json:", e)
            steps = ["Error parsing response."]

        return steps
