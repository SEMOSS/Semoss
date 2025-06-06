from typing import Dict, Optional
from abc import abstractmethod

from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...clients.client_initializer import google_initializer
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME


class AbstractAnthropicClient(AbstractTextGenerationClient):
    """
    Abstract class for Anthropic Client.
    """

    def __init__(
        self,
        model_name: str,
        service_account_credentials: Dict = None,
        service_account_key_file: str = None,
        region: str = None,
        project: str = None,
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
        )

        super().__init__(
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
        )

        self.model_name = model_name
        self.project = project
        self.region = region
        self.client = self._get_client()
        self.max_tokens = max_tokens
        self.safety_settings = safety_settings or {}

    @abstractmethod
    def ask_call(self, **kwargs) -> AskModelEngineResponse:
        pass

    @abstractmethod
    def _get_client(self):
        pass
