from typing import List
from ....constants import InstructModelEngineResponse
import pandas as pd


class Instruct:
    def __init__(self, client):
        self.client = client

    def instruct(
        self,
        task: str,
        projectData,
        context: str = None,
        max_new_tokens: int = 2000,
        prefix: str = "",
        **kwargs,
    ) -> InstructModelEngineResponse:
        """
        Handles the 'instruct' operation.
        """
        print("Executing Instruct Operation...")

        projects_df = self.convert_data_to_dataframe(projectData)

        task_target, detect_task_response = self._detect_task_target(
            task, context, max_new_tokens, prefix, **kwargs
        )

        steps_data, decompose_response = self._decompose_task(
            task, task_target, context, max_new_tokens, prefix, **kwargs
        )

        project_ids_data, align_tasks_response = self._align_tasks(
            steps_data, projects_df
        )

        final_data = self.combine_projects_and_steps(project_ids_data, steps_data)

        final_response = InstructModelEngineResponse()
        final_response.response = final_data
        final_response.prompt_tokens = align_tasks_response.prompt_tokens

        final_response.response_tokens = 1
        warnings = [detect_task_response.warning, decompose_response.warning]
        final_response.warning = "\n\n".join(filter(None, warnings))

        return final_response

    def _align_tasks(self, tasks: List[str], projects: pd.DataFrame):
        print("Aligning Steps...")

        # Convert tasks list to a numbered string
        tasks_str = "\n".join([f"{i+1}. {task}" for i, task in enumerate(tasks)])

        # Convert projects dataframe to a list of dictionaries
        projects_list = projects.to_dict("records")

        # Convert projects list to a string
        projects_str = "\n".join(
            [
                f"Project ID: {proj['project_id']}\n"
                f"Project Name: {proj['project_name']}\n"
                f"Description: {proj['description']}\n"
                for proj in projects_list
            ]
        )

        system_message = (
            "You are an AI assistant tasked with matching each task step to the most relevant project based on its description.\n\n"
            "### Tasks:\n"
            f"{tasks_str}\n\n"
            "### Projects:\n"
            f"{projects_str}\n"
            "### Instructions:\n"
            "- Analyze each task step and find the project whose description best matches the task.\n"
            "- Return a JSON array where each element corresponds to the project ID matching each task step.\n"
            f"- The JSON array MUST have the same length as the tasks list. The length of the task list is {len(tasks)}\n"
            '- **Output Format**: ["project_id_1", "project_id_2", "project_id_3", ...]\n'
            "- **Do not** include any additional text or explanation.\n"
        )

        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": ""},
        ]

        prompt_payload, max_new_tokens, align_tasks_response = (
            self.client.check_token_limits(prompt_payload=messages, max_new_tokens=3000)
        )

        payload = {
            "messages": prompt_payload,
            "temperature": 0.1,
            "top_p": 0.2,
            "max_tokens": max_new_tokens,
            "stream": False,
        }

        response = self.client.inference_call(prefix="", **payload)

        parsed_response = self.parse_response(response)

        return parsed_response, align_tasks_response

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
        import json

        try:
            steps = json.loads(response)
        except json.JSONDecodeError as e:
            print("Error parsing response with json:", e)
            steps = ["Error parsing response."]

        return steps

    def clean_raw_strings(self, s):
        """
        Removes the raw string notation r'''...''' from a string.
        """
        if isinstance(s, str):
            if s.startswith("r'''") and s.endswith("'''"):
                return s[4:-3]
        return s

    def combine_projects_and_steps(self, projects: List[str], steps: List[str]):
        if len(projects) != len(steps):
            print("PROBLEM!: The number of projects and steps must be equal.")
            print(f"There are {len(projects)} Projects:", projects)
            print(f"There are {len(steps)} Steps:", steps)
            raise ValueError("The number of projects and steps must be equal.")
        return [
            {"project_id": project, "step": step}
            for project, step in zip(projects, steps)
        ]

    def convert_data_to_dataframe(self, data_list):
        cleaned_data_list = []
        for item in data_list:
            cleaned_item = {}
            for key, value in item.items():
                # Clean both the key and the value
                cleaned_key = self.clean_raw_strings(key)
                cleaned_value = self.clean_raw_strings(value)
                cleaned_item[cleaned_key] = cleaned_value
            cleaned_data_list.append(cleaned_item)
        # Convert the cleaned list of dictionaries into a DataFrame
        df = pd.DataFrame(cleaned_data_list)
        return df
