# Deloitte GenAI Python bindings.
# import os
# from typing import Optional, TYPE_CHECKING
# from .utils._package_checker import check_pacakges

# utility to get the client
def get_client(client_type = '', **kwargs):
    if (client_type == 'openai'):
        return OpenAiClient(**kwargs)
    elif (client_type == 'azure-openai'):
        return AzureOpenAiClient(**kwargs)
    elif (client_type == 'text-gen'):
        return TextGenClient(**kwargs)
    else:
        raise ValueError('Client type has not been defined.')

# register all the clients in the init 
from genai_client.client_resources import (
    AzureOpenAiClient,
    OpenAiClient,
    TextGenClient,
)