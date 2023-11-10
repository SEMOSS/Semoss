# Deloitte GenAI Python bindings.

# register all the clients in the init 
from .text_generation import (
    AzureOpenAiClient,
    OpenAiClient,
    TextGenClient,
    BedrockClient,
    VertexClient
)

from .embedders import (
    LocalEmbedder,
    OpenAiEmbedder,
    AzureOpenAiEmbedder
)

from .tokenizers import (
    HuggingfaceTokenizer,
    OpenAiTokenizer
)

def get_text_gen_client(client_type, **kwargs):
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
    else:
        raise ValueError('Client type has not been defined.')
    
def get_embedder(embedder_type, **kwargs):
    '''
    Utility method to  get the appropriate embedder based on the Model Engine
    '''
    if (embedder_type == 'OPEN_AI'):
        is_azure = kwargs.pop('IS_AZURE_OPEN_AI')
        if is_azure != None and is_azure:
            return AzureOpenAiEmbedder(**kwargs)
        else:
            return OpenAiEmbedder(**kwargs)
    elif (embedder_type == 'EMBEDDED') or (encoder_type == 'TEXT_GENERATION'):
        LocalEmbedder(**kwargs)

def get_tokenizer(encoder_type:str, encoder_name, max_tokens):
    '''
    Utility method to  get the appropriate tokenizer based on the Model Engine
    '''
    if (encoder_type == 'EMBEDDED') or (encoder_type == 'TEXT_GENERATION'):
        return HuggingfaceTokenizer(encoder_name = encoder_name, max_tokens = max_tokens)
    elif (encoder_type == 'OPEN_AI'):
        return OpenAiTokenizer(encoder_name = encoder_name, max_tokens = max_tokens)
    else:
        raise ValueError('Encoder type has not been defined.')
