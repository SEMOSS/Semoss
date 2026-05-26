from typing import Union, Dict, Any, Optional, List
import json
import os
from string import Template
from abc import ABC, abstractmethod
from pydantic import BaseModel
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


class ModelLimits(BaseModel):
    context_window: Optional[int] = None
    max_input_tokens: Optional[int] = None
    max_completion_tokens: Optional[int] = None


class AbstractTextGenerationClient(ABC):
    # loads all the templates
    # fills the templates and gives information back
    def __init__(
        self,
        template: Optional[Union[Dict, str]] = None,
        template_name: Optional[str] = None,
        **kwargs: Any,
    ):
        self.model_name = kwargs.pop("model_name", None)
        if self.model_name is None:
            raise ValueError("model_name must be provided.")

        self.model_limits = self._get_model_limits(kwargs)

        self.template_name = template_name
        self.templates = {}
        self._handle_template_args(template)

        tokens_param_name = kwargs.pop("tokens_param_name", None)
        if not tokens_param_name:
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
                "max_completion_tokens",
            )

        thinking = kwargs.pop("thinking", False)
        if thinking is not None and thinking is not isinstance(thinking, bool):
            try:
                thinking = string_to_bool(thinking)
            except ValueError:
                thinking = False
        thinking_budget = kwargs.pop("thinking_budget", None)

        self.model_settings = ModelSettings(
            model_name=self.model_name,
            context_window=kwargs.get("context_window", None),
            max_completion_tokens=kwargs.get("max_completion_tokens", None),
            max_input_tokens=kwargs.get("max_input_tokens", None),
            ai_role=kwargs.pop("ai_role", None),
            user_role=kwargs.pop("user_role", None),
            system_role=kwargs.pop("system_role", None),
            chat_type=kwargs.pop("chat_type", None),
            model_type=kwargs.pop("model_type", None),
            tokens_param_name=tokens_param_name,
            thinking=thinking,
            thinking_budget=thinking_budget,
            global_param_override=kwargs.pop("global_param_override", None),
            modalities=kwargs.pop("modalities", None),
        )

    def _handle_template_args(self, template):
        """This may not be used anymore.."""
        # if the user does not provide a template, we default to chat_templates.json
        if template == None:
            script_directory = os.path.dirname(os.path.abspath(__file__))
            chat_templates = os.path.join(script_directory, "chat_templates.json")
            template = chat_templates

        # the user should be able to pass, their own file (json) or dictionary
        if isinstance(template, str):
            # since its a string, we assume its path and need to validate that its valid
            if os.path.exists(template) == False:
                raise FileNotFoundError(f"The file '{template}' does not exist.")

            self.template_file = template
            with open(template) as da_file:
                file_contents = da_file.read()
                self.templates = json.loads(file_contents)
        elif isinstance(template, dict):
            self.template_file = None
            self.templates = template

    def _get_model_limits(self, smss_args) -> ModelLimits:
        """
        Returns the model limits for the given  model.
        These only set limits that are preset in the SMSS file.
        If a model does not have these limits set, they should be resolved in the given client class.
        Only piloting this for google genai for now..
        """
        context_window = smss_args.get("context_window", None)
        max_input_tokens = smss_args.get("max_input_tokens", None)
        max_completion_tokens = smss_args.get("max_completion_tokens", None)
        if max_completion_tokens is None:
            max_completion_tokens = smss_args.get("max_tokens", None)

        return ModelLimits(
            context_window=context_window,
            max_input_tokens=max_input_tokens,
            max_completion_tokens=max_completion_tokens,
        )

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


# TODO remove once no errors are happening
class BaseClient(AbstractTextGenerationClient):
    pass
