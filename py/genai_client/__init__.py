# Deloitte GenAI Python bindings.
from typing import (
    Any
)

# register all the clients in the init


def __getattr__(name: str) -> Any:
    if name == "AzureOpenAiClient":
        from .text_generation.openai_clients import AzureOpenAiClient
        return AzureOpenAiClient
    elif name == "OpenAiClient":
        from .text_generation.openai_clients import OpenAiClient
        return OpenAiClient
    elif name == "TextGenClient":
        from .text_generation.textgen_client import TextGenClient
        return TextGenClient
    elif name == "BedrockClient":
        from .text_generation.bedrock_client import BedrockClient
        return BedrockClient
    elif name == "VertexClient":
        from .text_generation.google_clients.vertex_controller import VertexAiClientController as VertexClient
        return VertexClient
    elif name == "ImageGenClient":
        from .image_generation.imagegen_client import ImageGenClient
        return ImageGenClient

    elif name == "LocalEmbedder":
        from .embedders.local_embedder import LocalEmbedder
        return LocalEmbedder
    elif name == "OpenAiEmbedder":
        from .embedders.openai_embedder import OpenAiEmbedder
        return OpenAiEmbedder
    elif name == "AzureOpenAiEmbedder":
        from .embedders.azure_openai_embedder import AzureOpenAiEmbedder
        return AzureOpenAiEmbedder
    elif name == "TextEmbeddingsInference":
        from .embedders.textgen_embedder import TextEmbeddingsInference
        return TextEmbeddingsInference
    elif name == "VertexAiEmbedder":
        from .embedders.vertex_embedder import VertexAiEmbedder
        return VertexAiEmbedder

    elif name == "OpenAiTokenizer":
        from .tokenizers.openai_tokenizer import OpenAiTokenizer
        return OpenAiTokenizer
    elif name == "HuggingfaceTokenizer":
        from .tokenizers.huggingface_tokenizer import HuggingfaceTokenizer
        return HuggingfaceTokenizer

    else:
        raise AttributeError(f"Could not find: {name}")


def get_text_gen_client(client_type, **kwargs):
    '''
    Utility method to  get the appropriate client based on the Model Engine
    '''
    if (client_type == 'OPEN_AI'):
        is_azure = kwargs.pop('IS_AZURE_OPEN_AI')
        if is_azure is not None and is_azure:
            from .text_generation.openai_clients import AzureOpenAiClient
            return AzureOpenAiClient(**kwargs)
        else:
            from .text_generation.openai_clients import OpenAiClient
            return OpenAiClient(**kwargs)
    elif (client_type == 'TEXT_GENERATION'):
        from .text_generation.textgen_client import TextGenClient
        return TextGenClient(**kwargs)
    elif (client_type == 'BEDROCK'):
        from .text_generation.bedrock_client import BedrockClient
        return BedrockClient(**kwargs)
    elif (client_type == 'VERTEX'):
        from .text_generation.google_clients.vertex_controller import VertexAiClientController as VertexClient
        return VertexClient(**kwargs)
    else:
        raise ValueError('Client type has not been defined.')


def get_embedder(embedder_type, **kwargs):
    '''
    Utility method to  get the appropriate embedder based on the Model Engine
    '''
    if (embedder_type == 'OPEN_AI'):
        is_azure = kwargs.pop('IS_AZURE_OPEN_AI')
        if is_azure is not None and is_azure:
            from .embedders.azure_openai_embedder import AzureOpenAiEmbedder
            return AzureOpenAiEmbedder(**kwargs)
        else:
            from .embedders.openai_embedder import OpenAiEmbedder
            return OpenAiEmbedder(**kwargs)
    elif (embedder_type == 'EMBEDDED'):
        from .embedders.local_embedder import LocalEmbedder
        return LocalEmbedder(**kwargs)
    elif (embedder_type == 'TEXT_GENERATION'):
        from .embedders.textgen_embedder import TextEmbeddingsInference
        return TextEmbeddingsInference(**kwargs)
    elif (embedder_type == 'VERTEX'):
        from .embedders.vertex_embedder import VertexAiEmbedder
        return VertexAiEmbedder(**kwargs)
    else:
        raise ValueError('Embedder type has not been defined.')


def get_tokenizer(tokenizer_type: str, tokenizer_name, max_tokens):
    '''
    Utility method to  get the appropriate tokenizer based on the Model Engine
    '''
    if (tokenizer_type == 'EMBEDDED') or (tokenizer_type == 'TEXT_GENERATION'):
        from .tokenizers.huggingface_tokenizer import HuggingfaceTokenizer
        return HuggingfaceTokenizer(encoder_name=tokenizer_name, max_tokens=max_tokens)
    elif (tokenizer_type == 'OPEN_AI'):
        from .tokenizers.openai_tokenizer import OpenAiTokenizer
        return OpenAiTokenizer(encoder_name=tokenizer_name, max_tokens=max_tokens)
    # putting this for now, need to implement vertex tokenizer. this will fall back to cl100k_base
    elif (tokenizer_type == 'VERTEX'):
        from .tokenizers.openai_tokenizer import OpenAiTokenizer
        return OpenAiTokenizer(encoder_name=tokenizer_name, max_tokens=max_tokens)
    else:
        raise ValueError('Tokenizer type has not been defined.')


__all__ = [
    'AzureOpenAiClient',
    'OpenAiClient',
    'TextGenClient',
    'ImageGenClient',
    'BedrockClient',
    'VertexClient',
    'LocalEmbedder',
    'OpenAiEmbedder',
    'AzureOpenAiEmbedder',
    'TextEmbeddingsInference',
    'VertexAiEmbedder',
    'OpenAiTokenizer',
    'HuggingfaceTokenizer',
    'get_text_gen_client',
    'get_embedder',
    'get_tokenizer'
]
