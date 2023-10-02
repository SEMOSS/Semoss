# utility to get the client

# register all the clients in the init
from .encoders.huggingface_encoder import (
    HuggingFaceEncoder
)
from .encoders.openai_encoder import (
    OpenaiEncoderClass
)
from .faiss.faiss_database import (
    FAISSDatabase
)

# make it easy to get the encoder
def get_encoder(encoder_type = '', **kwargs):
    if (encoder_type == 'openai'):
        return OpenaiEncoderClass(**kwargs)
    elif (encoder_type == 'huggingface'):
        return HuggingFaceEncoder(**kwargs)
    else:
        raise ValueError('Encoder type has not been defined.')