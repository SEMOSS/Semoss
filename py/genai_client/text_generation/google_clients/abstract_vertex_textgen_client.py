from typing import Any, Dict, Optional
from vertexai.language_models import ChatMessage

from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...clients.client_initializer import google_initializer
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME, FULL_PROMPT
from ...usage_ceiling import count_prompt_tokens


class AbstractVertextAiTextGeneration(AbstractTextGenerationClient):
    """
    Abstract class for Vertex AI inference.
    """

    def __init__(
        self,
        model_name: str,
        service_account_credentials: Dict = None,
        service_account_key_file: str = None,
        region: str = None,
        project: str = None,
        api_endpoint: Optional[str] = None,
        max_tokens: int = None,
        safety_settings: Optional[Dict] = None,
        **kwargs,
    ):

        # initialize the google aiplatform connection
        google_initializer(
            region=region,
            service_account_credentials=service_account_credentials,
            service_account_key_file=service_account_key_file,
            project=project,
            api_endpoint=api_endpoint,
        )

        super().__init__(
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
        )

        self.model_name = model_name
        self.client = self._get_client()
        self.max_tokens = max_tokens
        self.safety_settings = safety_settings or {}

    @classmethod
    def ask_call(self, **kwargs) -> AskModelEngineResponse:
        pass

    def fit_max_new_tokens(
        self, max_new_tokens: Optional[int], *parts: Any, message_count: int = 1
    ) -> Optional[int]:
        """The output maximum to send under the usage ceiling of the call in
        progress: ``max_new_tokens``, lowered to what the prompt leaves.

        ``parts`` are the context, history and question as sent. The prompt
        is estimated, since these clients have no tokenizer. Raises
        UsageLimitDoesNotFitError when the prompt leaves too little for output, which
        ``ask`` turns into the USAGE_LIMIT_DOES_NOT_FIT error response.

        The Vertex clients share this one helper, as they build no single
        request to fit; their ``AskModelEngineResponse`` carries no
        ``usageCapped`` mark, so only the maximum is fitted.
        """
        fit = self.fit_output_to_usage_ceiling(
            max_new_tokens,
            lambda: count_prompt_tokens(list(parts), message_count=message_count),
        )
        if fit is None or not fit.lowered:
            return max_new_tokens
        return fit.max_tokens

    @classmethod
    def _get_client(self):
        pass
