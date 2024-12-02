from typing import List, Union, Optional
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
        max_new_tokens: Optional[int] = None,
        max_completion_tokens: Optional[int] = None,
        prefix: str = "",
        **kwargs,
    ) -> InstructModelEngineResponse:
        # Until we fully remove max_new_tokens
        max_completion_tokens = max_completion_tokens or max_new_tokens
        """Handles the 'instruct' operation"""
        print("Executing Instruct Operation...")

        projects_df_raw = self.convert_data_to_dataframe(projectData)
        projects_df = self.scrub_df(projects_df_raw)
        # Identify the target audience for the task
        detect_task_response = self._detect_task_target(
            question=task,
            context=context,
            prefix=prefix,
            max_completion_tokens=max_completion_tokens,
            **kwargs,
        )
        # Decompose the task into a sequence of steps
        decompose_response = self._decompose_task(
            question=task,
            task_target=detect_task_response.response,
            context=context,
            prefix=prefix,
            max_completion_tokens=max_completion_tokens,
            **kwargs,
        )
        # Align the steps with the most relevant projects
        align_tasks_response = self._align_tasks(
            decompose_response.response, projects_df
        )
        # Generate descriptions for each step
        descriptions = self.generate_descriptions(
            decompose_task_results=decompose_response.response, task=task
        )
        # Generate expected inputs and outputs for each step
        io_results = self.generate_inputs_and_outputs(
            decompose_task_results=decompose_response.response, task=task
        )
        # Merge the projects and steps into a single list of dictionaries
        final_data = self.combine_projects_and_steps(
            align_tasks_response.response,
            decompose_response.response,
            descriptions,
            io_results,
        )

        final_response = InstructModelEngineResponse()
        final_response.response = final_data
        final_response.prompt_tokens = align_tasks_response.prompt_tokens
        final_response.response_tokens = align_tasks_response.response_tokens
        warnings = [detect_task_response.warning, decompose_response.warning]
        final_response.warning = "\n\n".join(filter(None, warnings))

        return final_response

    def _align_tasks(self, tasks: List[str], projects: pd.DataFrame):
        print("Aligning Steps...")

        tasks_str = "\n".join([f"{i+1}. {task}" for i, task in enumerate(tasks)])

        projects_list = projects.to_dict("records")

        projects_str = "\n".join(
            [
                f"Project ID: {proj['project_id']}\n"
                f"Project Name: {proj['project_name']}\n"
                f"Description: {proj['description']}\n"
                for proj in projects_list
            ]
        )

        system_message = (
            "You are an AI assistant tasked with matching each task step to the most relevant project(s) based on its description.\n\n"
            "### Tasks:\n"
            f"{tasks_str}\n\n"
            "### Projects:\n"
            f"{projects_str}\n"
            "### Instructions:\n"
            "- Analyze each task step and find all projects whose descriptions match the task requirements.\n"
            "- If multiple projects can be used to complete a task, include all relevant project IDs.\n"
            "- Return a JSON array where each element is either a single project ID string or an array of project IDs.\n"
            "- Each step MUST be matched to at least one project.\n"
            f"- The JSON array MUST have the same length as the tasks list. The length of the task list is {len(tasks)}\n"
            '- **Output Format**: ["project_id_1", ["project_id_2", "project_id_3"], "project_id_4", ...]\n'
            "- **Do not** include any additional text or explanation.\n"
        )

        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": ""},
        ]

        prompt_payload, adjusted_max_completion_tokens, align_tasks_response = (
            self.client.check_token_limits(prompt_payload=messages)
        )

        payload = {
            "messages": prompt_payload,
            "temperature": 0.1,
            "top_p": 0.2,
            "max_tokens": adjusted_max_completion_tokens,
            "stream": False,
        }

        response = self.client.inference_call(prefix="", **payload)

        response_tokens = self.client.tokenizer.count_tokens(response)

        parsed_response = self.parse_alignment_response(response)

        align_tasks_response.response = parsed_response
        align_tasks_response.response_tokens = response_tokens

        return align_tasks_response

    def parse_alignment_response(self, response):
        """
        Parses the alignment response which may contain single project IDs or arrays of project IDs.

        Args:
            response (str): JSON string containing project alignments

        Returns:
            List[Union[str, List[str]]]: List where each element is either a project ID string
                                    or a list of project ID strings
        """
        import json

        try:
            alignments = json.loads(response)
            # Normalizing response
            normalized = []
            for item in alignments:
                if isinstance(item, list):
                    normalized.append(item)
                elif isinstance(item, str):
                    normalized.append(item)
                else:
                    raise ValueError(f"Invalid alignment format: {item}")

            return normalized

        except json.JSONDecodeError as e:
            print("Error parsing response with json:", e)
            return ["Error parsing response."]
        except ValueError as e:
            print("Error validating response format:", e)
            return ["Error validating response format."]

    def combine_projects_and_steps(
        self,
        projects: List[Union[str, List[str]]],
        steps: List[str],
        descriptions: List[str],
        io_results: List[dict],
    ):
        """Combines projects and steps, handling cases where a step may have multiple projects."""
        if len(projects) != len(steps):
            raise ValueError("The number of projects and steps must be equal.")

        if len(descriptions) != len(steps):
            raise ValueError("The number of descriptions and steps must be equal.")

        result = []
        for project, step, description, io in zip(
            projects, steps, descriptions, io_results
        ):
            if isinstance(project, list):
                result.append(
                    {
                        "project_ids": project,
                        "step": step,
                        "description": description,
                        "io": io,
                    }
                )
            else:
                result.append(
                    {
                        "project_ids": [project],
                        "step": step,
                        "description": description,
                        "io": io,
                    }
                )

        return result

    def generate_inputs_and_outputs(
        self, decompose_task_results: List[str], task: str
    ) -> List[dict]:
        """Generates expected inputs and outputs for each task step."""
        system_message = (
            f"For the task: '{task}', analyze each step and determine its expected inputs and outputs.\n\n"
            "### Instructions:\n"
            "- For each step, identify required inputs and expected outputs\n"
            "- Keep inputs and outputs concise and specific\n"
            "- Return a JSON array where each element has 'inputs' and 'outputs' arrays\n"
            f"- The array MUST have {len(decompose_task_results)} items\n"
            '- **Output Format**: [{"inputs": ["input1", "input2"], "outputs": ["output1"]}, ...]\n'
            "- **Do not** include any additional text or explanation\n"
        )

        steps_str = "\n".join(
            [f"{i+1}. {step}" for i, step in enumerate(decompose_task_results)]
        )
        user_message = f"### Steps:\n{steps_str}\n### Response:"

        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_message},
        ]

        prompt_payload, adjusted_max_completion_tokens, _ = (
            self.client.check_token_limits(prompt_payload=messages)
        )

        payload = {
            "messages": prompt_payload,
            "temperature": 0.1,
            "top_p": 0.2,
            "max_tokens": adjusted_max_completion_tokens,
            "stream": False,
        }

        response = self.client.inference_call(prefix="", **payload)

        try:
            import json

            io_specifications = json.loads(response)

            if len(io_specifications) != len(decompose_task_results):
                raise ValueError(
                    "Number of IO specifications does not match number of steps"
                )

            for spec in io_specifications:
                if (
                    not isinstance(spec, dict)
                    or "inputs" not in spec
                    or "outputs" not in spec
                ):
                    raise ValueError("Invalid IO specification format")
                if not isinstance(spec["inputs"], list) or not isinstance(
                    spec["outputs"], list
                ):
                    raise ValueError("Inputs and outputs must be arrays")

            return io_specifications

        except json.JSONDecodeError:
            print("Error parsing response with json")
            return [{"inputs": ["Error"], "outputs": ["Error"]}] * len(
                decompose_task_results
            )
        except ValueError as e:
            print(f"Error validating response: {e}")
            return [{"inputs": ["Error"], "outputs": ["Error"]}] * len(
                decompose_task_results
            )

    def generate_descriptions(
        self, decompose_task_results: List[str], task: str
    ) -> List[str]:
        """Generates descriptions explaining the importance of each step in the decomposed task."""

        system_message = (
            f"For each step in completing the task: '{task}', explain why it is important in just one sentence.\n\n"
            "### Instructions:\n"
            "- Analyze each step and explain its significance to the overall task\n"
            "- Each explanation should be clear and specific\n"
            "- Focus on the value and purpose of each step\n"
            "- Present the explanations in JSON array format\n"
            "- Each explanation must directly correspond to its step\n"
            f"- The array MUST have {len(decompose_task_results)} items\n"
            '- **Output Format**: ["Description 1", "Description 2", ...]\n'
            "- **Do not** include any additional text or explanation\n"
        )
        steps_str = "\n".join(
            [f"{i+1}. {step}" for i, step in enumerate(decompose_task_results)]
        )

        user_message = f"### Steps:\n{steps_str}\n### Response:"

        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_message},
        ]

        prompt_payload, adjusted_max_completion_tokens, _ = (
            self.client.check_token_limits(prompt_payload=messages)
        )

        payload = {
            "messages": prompt_payload,
            "temperature": 0.1,
            "top_p": 0.2,
            "max_tokens": adjusted_max_completion_tokens,
            "stream": False,
        }

        response = self.client.inference_call(prefix="", **payload)

        try:
            import json

            descriptions = json.loads(response)

            if len(descriptions) != len(decompose_task_results):
                raise ValueError(
                    "Number of descriptions does not match number of steps"
                )

            return descriptions

        except json.JSONDecodeError:
            print("Error parsing response with json")
            return ["Error: Could not generate description."] * len(
                decompose_task_results
            )
        except ValueError as e:
            print(f"Error validating response: {e}")
            return ["Error: Invalid response format."] * len(decompose_task_results)

    def _detect_task_target(
        self,
        question: str,
        context: str,
        prefix: str,
        max_completion_tokens: int = None,
        **kwargs,
    ):
        print("Detecting Task Target...")
        temp = kwargs.get("temperature", 0.1)
        top_p = kwargs.get("top_p", 0.2)

        system_message = f"""Imagine you have a task {question}. Explain in a single sentence who the task should be intended for."""

        messages = []

        if context:
            messages.append({"role": "system", "content": context})

        messages.append({"role": "system", "content": system_message})

        messages.append({"role": "user", "content": ""})

        prompt_payload, adjusted_max_completion_tokens, detect_task_response = (
            self.client.check_token_limits(
                prompt_payload=messages, user_max_tokens=max_completion_tokens
            )
        )

        updated_kwargs = kwargs.copy()
        updated_kwargs.update(
            {
                "messages": prompt_payload,
                "temperature": temp,
                "top_p": top_p,
                "max_tokens": adjusted_max_completion_tokens,
                "stream": False,
            }
        )

        response = self.client.inference_call(prefix=prefix, **updated_kwargs).strip()

        detect_task_response.response = response

        return detect_task_response

    def _decompose_task(
        self,
        question: str,
        task_target: str,
        context: str,
        prefix: str,
        max_completion_tokens: int = None,
        **kwargs,
    ):
        print("Decomposing Task...")
        temp = kwargs.get("temperature", 0.1)
        top_p = kwargs.get("top_p", 0.2)
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

        prompt_payload, adjusted_max_completion_tokens, decompose_response = (
            self.client.check_token_limits(
                prompt_payload=messages, user_max_tokens=max_completion_tokens
            )
        )

        updated_kwargs = kwargs.copy()
        updated_kwargs.update(
            {
                "messages": prompt_payload,
                "temperature": temp,
                "top_p": top_p,
                "max_tokens": adjusted_max_completion_tokens,
                "stream": False,
            }
        )

        response = self.client.inference_call(prefix=prefix, **updated_kwargs)

        parsed_response = self.parse_response(response)

        decompose_response.response = parsed_response

        return decompose_response

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

    def scrub_df(self, df):
        scrubbed_df = df[["description", "project_id", "project_name"]]
        return scrubbed_df
