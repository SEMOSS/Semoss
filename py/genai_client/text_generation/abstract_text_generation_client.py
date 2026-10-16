from typing import Union, Dict, Any, Optional, List, Callable
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
from ..message_builders.semoss_base.document_input import DocumentInputProcessor
from ..usage_ceiling import (
    USAGE_CEILING,
    CeilingFit,
    PromptCount,
    UsageCeiling,
    UsageLimitDoesNotFitError,
    current_usage_ceiling,
    fit_to_usage_ceiling,
    usage_ceiling_scope,
)
from .model_engine_exception import ErrorDetails, ModelEngineException


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
            native_document_mime_types=kwargs.pop("native_document_mime_types", None),
            preserve_reasoning_content=kwargs.pop("preserve_reasoning_content", False),
            chat_reasoning_mode=kwargs.pop("chat_reasoning_mode", "reasoning_effort"),
        )

        self._document_input_processor = DocumentInputProcessor()

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

        # Internal engine metadata is consumed here, never sent to the provider.
        input_modalities = kwargs.pop("_semoss_input_modalities", None)
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

        return self._document_input_processor.prepare(
            semoss_messages,
            model_settings,
            source_messages=message_json,
            input_modalities=input_modalities,
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
        # the usage ceiling is held for this call only and never reaches ask_call
        ceiling = UsageCeiling.from_value(kwargs.pop(USAGE_CEILING, None))
        with usage_ceiling_scope(ceiling) as ceiling_call:
            try:
                response = self.ask_call(*args, **kwargs)
            except UsageLimitDoesNotFitError as e:
                # the refusal goes back as the USAGE_LIMIT_DOES_NOT_FIT error response,
                # which Java reads to release the call's reservation
                response = ModelEngineException(
                    error=e, client=type(self).__name__, model=str(self.model_name)
                ).parse_error()
        # usageCapped tells the caller the answer was cut by what is left on a
        # usage limit, not by its own maximum
        if (
            ceiling_call is not None
            and isinstance(response, AskModelEngineResponse2)
            and ceiling_call.capped(response.response_tokens)
        ):
            response.usage_capped = True
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

    def fit_output_to_usage_ceiling(
        self,
        requested_max: Optional[int],
        count_prompt: Callable[[], PromptCount],
        can_cap: bool = True,
    ) -> Optional[CeilingFit]:
        """Fit the call in progress to the usage ceiling of this ask.

        Every provider client applies the cap through this one method, so the
        rules of ``usage_ceiling`` hold the same way for each.

        Clients call this with their finished provider request: the output
        maximum it carries and a function that counts its prompt (called only
        when a ceiling applies). Returns None without a ceiling. Otherwise the
        returned fit holds the output maximum to send; when ``fit.lowered`` the
        client sets it on the request and lowers any thinking budget with
        ``fit_thinking_budget``. Raises UsageLimitDoesNotFitError when the prompt
        leaves less than ``min_output_tokens``; the client's own error handling
        or ``ask`` turns it into the USAGE_LIMIT_DOES_NOT_FIT error response.
        """
        return fit_to_usage_ceiling(requested_max, count_prompt, can_cap=can_cap)

    @staticmethod
    def usage_ceiling_min_output_tokens() -> int:
        """The output the ceiling of the call in progress keeps for the answer,
        for fitting thinking budgets. 1 without a ceiling, so the budget only has
        to stay below the output maximum."""
        call = current_usage_ceiling()
        return call.ceiling.min_output_tokens if call is not None else 1

    def embeddings(self, *args: Any, **kwargs: Any) -> Dict:
        # accepted so every engine call can pass it; nothing is embedded here
        kwargs.pop(USAGE_CEILING, None)
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
        kwargs.pop(USAGE_CEILING, None)
        return {
            "response": "This model does not support multi modal embeddings.",
            "implemented": False,
        }


# TODO remove once no errors are happening
class BaseClient(AbstractTextGenerationClient):
    pass
