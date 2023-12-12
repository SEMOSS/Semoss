from typing import Dict, Optional, List, Union
from abc import abstractclassmethod
from vertexai.language_models import ChatMessage

from ..base_client import BaseClient
from ...clients.client_initializer import google_initializer
from ...constants import (
    ModelEngineResponse,
    TEMPLATE,
    TEMPLATE_NAME,
    FULL_PROMPT
)

class AbstractVertextAiTextGeneration(BaseClient):
    """
    Abstract class for Vertex AI inference.
    """

    def __init__(
        self,      
        model_name:str ,
        service_account_credentials:Dict=None,
        service_account_key_file:str=None,
        region:str=None,
        project:str=None,
        **kwargs,
    ):
        
        # initialize the google aiplatform connection
        google_initializer(
            region=region,
            service_account_credentials=service_account_credentials,
            service_account_key_file=service_account_key_file,
            project=project
        )
        
        super().__init__(
            template = kwargs.pop(
                TEMPLATE, 
                None
            ),
            template_name = kwargs.pop(
                TEMPLATE_NAME, 
                None
            )
        )
        
        self.model_name = model_name
        self.client = self._get_client()
    
    @abstractclassmethod
    def ask(
        self,
        **kwargs
    ):
        pass
    
    @abstractclassmethod
    def _get_client(
        self
    ):
        pass