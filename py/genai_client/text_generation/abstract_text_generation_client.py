from typing import Union, Dict, Any, Optional, List
import json
import os
from string import Template
from abc import ABC, abstractmethod
from ..constants import (
    AskModelEngineResponse,
    AskModelEngineResponse2,
    EmbeddingsModelEngineResponse,
)
from ..message_builders.semoss_base.semoss_message_builder import SEMOSSMessageBuilder
from ..message_builders.semoss_base.semoss_models import (
    ModelSettings,
    SEMOSSMessage,
)
from ..utils import string_to_bool
from .model_engine_exception import ErrorDetails


class AbstractTextGenerationClient(ABC):
    def __init__(
        self,
        template: Optional[Union[Dict, str]] = None,
        template_name: Optional[str] = None,
        **kwargs: Any,
    ):
        self.model_name = kwargs.pop("model_name", None)
        if self.model_name is None:
            raise ValueError("model_name must be provided.")

        self.template_name = template_name
        self.templates = {}
        self._handle_template_args(template)

        thinking = kwargs.pop("thinking", False)
        if thinking is not None and thinking is not isinstance(thinking, bool):
            try:
                thinking = string_to_bool(thinking)
            except ValueError:
                thinking = False
        thinking_budget = kwargs.pop("thinking_budget", None)

        tokens_param_name = next(
            (
                param
                for param in [
                    "max_completion_tokens",
                    "max_tokens",
                    "max_new_tokens",
                ]
                if param in kwargs
            ),
            "max_tokens",
        )

        self.model_settings = ModelSettings(
            model_name=self.model_name,
            context_window=kwargs.get("context_window", None),
            max_tokens=kwargs.get(tokens_param_name, None),
            ai_role=kwargs.pop("ai_role", None),
            user_role=kwargs.pop("user_role", None),
            system_role=kwargs.pop("system_role", None),
            chat_type=kwargs.pop("chat_type", None),
            model_type=kwargs.pop("model_type", None),
            thinking=thinking,
            thinking_budget=thinking_budget,
            global_param_override=kwargs.pop("global_param_override", None),
            modalities=kwargs.pop("modalities", None),
        )

    def _handle_template_args(self, template):
        """This may not be used anymore.."""
        if template == None:
            script_directory = os.path.dirname(os.path.abspath(__file__))
            chat_templates = os.path.join(script_directory, "chat_templates.json")
            template = chat_templates

        if isinstance(template, str):
            if os.path.exists(template) == False:
                raise FileNotFoundError(f"The file '{template}' does not exist.")

            self.template_file = template
            with open(template) as da_file:
                file_contents = da_file.read()
                self.templates = json.loads(file_contents)
        elif isinstance(template, dict):
            self.template_file = None
            self.templates = template

    def build_semoss_messages(
        self,
        model_settings: ModelSettings,
        **kwargs,
    ) -> List[SEMOSSMessage]:
        """Build SEMOSS Messages from the message_json format."""
        message_json = kwargs.pop("message_json", None)

        if not message_json:
            raise ValueError("message_json is required to build semoss messages.")

        param_map = {**kwargs}

        try:
            message_json = json.loads(message_json)
            semoss_messages = SEMOSSMessageBuilder().build_messages(
                input_messages=message_json,
                param_map=param_map,
                model_settings=model_settings,
            )
        except json.JSONDecodeError:
            try:
                decoded_string = message_json.replace('\\n",', '",')
                decoded_string = decoded_string.encode().decode("unicode_escape")

                message_json = json.loads(decoded_string)
                semoss_messages = SEMOSSMessageBuilder().build_messages(
                    input_messages=message_json,
                    param_map=param_map,
                    model_settings=model_settings,
                )
            except Exception as e:
                raise ValueError(f"Invalid JSON format in message_json.: {e}")

        return semoss_messages

    def get_template(self, template_name=None, **kwargs):
        if template_name in self.templates.keys():
            return self.templates[template_name]
        elif f"{self.model_name}.default.context" in self.templates:
            return self.templates[f"{self.model_name}.default.context"]
        elif f"{self.model_name}.default.nocontext" in self.templates:
            return self.templates[f"{self.model_name}.default.nocontext"]
        else:
            return None

    def add_template(self, template_name=None, template=None):
        assert template_name is not None
        if template_name not in self.templates:
            self.templates.update({template_name: template})
            print("template is set")
        else:
            print("template already exists")

    def write_templates(self, template_file=None):
        if template_file is None:
            template_file = self.template_file

        with open(template_file, "w") as f:
            json.dump(self.templates, f)

    def fill_template(self, template_name=None, **kwargs):
        assert template_name is not None
        this_template = self.get_template(template_name, **kwargs)
        if this_template is not None:
            return self.fill_context(this_template, **kwargs)
        else:
            return None, False

    # note, kwargs here is just a dictionary -- not a dictionary construction
    def fill_context(self, theContext, **kwargs):
        template = Template(theContext)
        output = template.substitute(**kwargs)
        # assumption -- if str substitution occures, then we dont need to user,system prompt ourselves
        if output != theContext:
            substitutions_made = True
        else:
            substitutions_made = False

        return output, substitutions_made

    def ask(self, *args: Any, **kwargs: Any) -> Dict:
        response = self.ask_call(*args, **kwargs)
        if isinstance(response, AskModelEngineResponse):
            return response.to_dict()
        if isinstance(response, AskModelEngineResponse2):
            return response.model_dump(exclude_none=True)
        elif isinstance(response, ErrorDetails):
            return response.model_dump()
        else:
            raise ValueError("Invalid response type from ask_call.")

    @abstractmethod
    def ask_call(self, *args: Any, **kwargs: Any) -> AskModelEngineResponse:
        pass

    def embeddings(self, *args: Any, **kwargs: Any) -> Dict:
        response = "This model does not support embeddings."

        numberOfTokensInResponse = 6
        try:
            self.tokenizer.count_tokens(response)
        except:
            pass

        model_engine_response = EmbeddingsModelEngineResponse(
            response=response, prompt_tokens=0, response_tokens=numberOfTokensInResponse
        )

        return model_engine_response.to_dict()

    def multi_modal_embeddings(self, *args: Any, **kwargs: Any) -> Dict:
        """Optional capability. Text generation clients do not embed by default."""
        return {
            "response": "This model does not support multi modal embeddings.",
            "implemented": False,
        }

    # Optional capability, same as multi_modal_embeddings above -- default to a
    # clean, catchable error for any client that doesn't override these, rather
    # than an AttributeError from a missing method reaching Java as an opaque
    # transport failure. AbstractPythonModelEngine.supportsBatch() (Java) gates
    # calls on ModelTypeEnum, which is coarser than "this specific client
    # implements batch" -- these defaults are the fallback for that gap.
    def _batch_not_supported(self):
        raise NotImplementedError(
            f"Batch is not supported for model '{self.model_name}' on this provider."
        )

    def submit_batch(self, requests, **kwargs) -> Dict:
        self._batch_not_supported()

    def get_batch_status(self, provider_batch_id: str, **kwargs) -> Dict:
        self._batch_not_supported()

    def get_batch_results(self, provider_batch_id: str, **kwargs) -> Dict:
        self._batch_not_supported()

    def list_batches(self, limit: int = 20, **kwargs) -> Dict:
        self._batch_not_supported()

    def cancel_batch(self, provider_batch_id: str, **kwargs) -> Dict:
        self._batch_not_supported()


# TODO remove once no errors are happening
class BaseClient(AbstractTextGenerationClient):
    pass
