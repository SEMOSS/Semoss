# Deloitte GenAI Python bindings.

import os
from contextvars import ContextVar
from typing import Optional, TYPE_CHECKING

from genai_client.client_resources import (
    OpenAiClient,
    FastChatClient,
    TextGenClient,
)

def get_client(client_type = '', **kwargs):
    if (client_type == 'openai'):
        return OpenAiClient(**kwargs)
    elif (client_type == 'fastchat'):
        return FastChatClient(**kwargs)
    elif (client_type == 'text-gen'):
        return TextGenClient(**kwargs)
    else:
        raise ValueError('Client type has not been defined.')
