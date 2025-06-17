from typing import Union, Dict, Any, Optional, List
import json
import os
from string import Template
from abc import ABC, abstractmethod
from pydantic import BaseModel
from ..constants import (
    AskModelEngineResponse,
    EmbeddingsModelEngineResponse,
    FULL_PROMPT,
)


class ModelLimits(BaseModel):
    context_window: Optional[int] = None
    max_input_tokens: Optional[int] = None
    max_completion_tokens: Optional[int] = None


class AskSettings(BaseModel):
    """
    Represents all of the conditional settings that affect the model call but are not passed
    as parameters to the model call itself.
    """

    full_prompt: Optional[List[Dict]] = None
    streaming: bool = False
    use_history: bool = True
    history: Optional[List[Dict]] = None
    image_url: Optional[List[str]] = None
    image_encoded: Optional[List[str]] = None


class AbstractTextGenerationClient(ABC):
    # loads all the templates
    # fills the templates and gives information back
    def __init__(
        self,
        template: Union[Dict, str] = None,
        template_name: str = None,
        **kwargs: Any,
    ):
        self.model_name = kwargs.get("model_name", None)
        self.model_limits = self._get_model_limits(kwargs)

        self.template_name = template_name
        self.templates = {}

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

    def get_ask_settings(
        self, history=None, use_history: bool = True, **kwargs
    ) -> AskSettings:
        """
        Get the ask settings from the provided keyword arguments.
        These are all settings that typically affect HOW I call the model.
        Not things I necissarily pass to the model call itself.
        """
        full_prompt = kwargs.pop(FULL_PROMPT, None)

        streaming = kwargs.pop("streaming", True)
        if not streaming:
            streaming = kwargs.pop("stream", True)

        image_url = kwargs.pop("image_url", None)
        if isinstance(image_url, str):
            image_url = [image_url]

        image_encoded = kwargs.pop("image_encoded", None)
        if isinstance(image_encoded, str):
            image_encoded = [image_encoded]

        if not use_history:
            history = None

        return AskSettings(
            full_prompt=full_prompt,
            streaming=streaming,
            history=history,
            image_url=image_url,
            image_encoded=image_encoded,
        )

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
        return self.ask_call(*args, **kwargs).to_dict()

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
