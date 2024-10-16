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
        final_response.response = [final_data]
        final_response.prompt_tokens = (
            detect_task_response.prompt_tokens + decompose_response.prompt_tokens
        )
        final_response.response_tokens = self.client.tokenizer.count_tokens(final_data)
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

        print("TASK TARGET: ", task_target)

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

                """
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

        start_index = response.find("{")
        end_index = response.rfind("}")
        if start_index != -1 and end_index != -1:
            output_data = response[start_index : end_index + 1]
        else:
            output_data = response.strip()

        return output_data, decompose_response
