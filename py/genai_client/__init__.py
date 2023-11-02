# Deloitte GenAI Python bindings.

# register all the clients in the init 
from genai_client.client_resources import (
    AzureOpenAiClient,
    OpenAiClient,
    TextGenClient,
    BedrockClient,
    VertexClient,
    EmbeddedEncoder
)

# register all the clients in the init 
from genai_client.tokenizers import *

def get_client(client_type = '', **kwargs):
    '''
    Utility method to  get the appropriate client based on the Model Engine
    '''
    if (client_type == 'OPEN_AI'):
        is_azure = kwargs.pop('IS_AZURE_OPEN_AI')
        if is_azure != None and is_azure:
            return AzureOpenAiClient(**kwargs)
        else:
            return OpenAiClient(**kwargs)
    elif (client_type == 'TEXT_GENERATION'):
        return TextGenClient(**kwargs)
    elif (client_type == 'BEDROCK'):
        return BedrockClient(**kwargs)
    elif (client_type == 'VERTEX'):
        return VertexClient(**kwargs)
    elif (client_type == 'embedded_encoder'):
        return EmbeddedEncoder(**kwargs)
    else:
        raise ValueError('Client type has not been defined.')

def get_tokenizer(encoder_type:str, encoder_name, encoder_max_tokens):
    '''
    Utility method to  get the appropriate tokenizer based on the Model Engine
    '''
    if (encoder_type == 'TEXT_GENERATION'):
        return HuggingfaceTokenizer(encoder_name = encoder_name, encoder_max_tokens = encoder_max_tokens)
    elif (encoder_type == 'OPEN_AI'):
        return OpenAiTokenizer(encoder_name = encoder_name, encoder_max_tokens = encoder_max_tokens)
    else:
        raise ValueError('Encoder type has not been defined.')
