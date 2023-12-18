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
    AzureOpenAiEmbedder,
    TextEmbeddingsInference,
    VertexAiEmbedder
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
    elif (embedder_type == 'EMBEDDED'):
        return LocalEmbedder(**kwargs)
    elif (embedder_type == 'TEXT_GENERATION'):
        return TextEmbeddingsInference(**kwargs)
    elif (embedder_type == 'VERTEX'):
        return VertexAiEmbedder(**kwargs)
    else:
        raise ValueError('Embedder type has not been defined.')

def get_tokenizer(tokenizer_type:str, tokenizer_name, max_tokens):
    '''
    Utility method to  get the appropriate tokenizer based on the Model Engine
    '''
    if (tokenizer_type == 'EMBEDDED') or (tokenizer_type == 'TEXT_GENERATION'):
        return HuggingfaceTokenizer(encoder_name = tokenizer_name, max_tokens = max_tokens)
    elif (tokenizer_type == 'OPEN_AI'):
        return OpenAiTokenizer(encoder_name = tokenizer_name, max_tokens = max_tokens)
    else:
        raise ValueError('Tokenizer type has not been defined.')
