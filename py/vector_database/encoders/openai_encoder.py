from typing import Any
import openai
import numpy as np
from .encoder_interface import EncoderInterface
import pandas

class OpenaiEncoderClass(EncoderInterface):

    def __init__(self, embedding_model:str = None, api_key:str = None) -> None:
        self.embedding_model = embedding_model
        self.api_key = api_key

    def get_embeddings(self, dataToEncode:Any, embedding_model:str = None, api_key:str = None) -> np.array:
        # store the original openai key so we can switch it back after encoding
        # We dont want to set a key if its just being used as a utility
        previously_defined_key = openai.api_key

        if api_key == None:
            api_key = self.api_key

        # check if the openai has been defined somewhere up the chain
        if (previously_defined_key == None):
            # if openai has not been imported beforehand, then we check if the call provided a key
            if (api_key == None):
                # if there is no api key defined, then we have to raise an error since we cannot make the call without it
                raise ValueError("Open AI key must be provided in order to run this method")
            else:
                # we set the key that was passed in
                openai.api_key = api_key
        
        # need to make sure the embedding model was passed
        if (embedding_model == None):
            if (self.embedding_model == None):
                raise ValueError("Please specify an embedding model in order to utilize this method")
            else:
                embedding_model = self.embedding_model
        # Determine what object was bassed in so we can pre-configure it before making the call
        ## NOTE - openai Embeddings call returns a List type
        if isinstance(dataToEncode, list):
            embedded_list = list(map(lambda x: self._make_openai_embedding_call(x, embedding_model=embedding_model), dataToEncode))     
        elif isinstance(dataToEncode, pandas.core.series.Series):
            embedded_list = dataToEncode.apply(lambda x: self._make_openai_embedding_call(x, embedding_model=embedding_model))
        elif isinstance(dataToEncode, str):
            embedded_list = self._make_openai_embedding_call(dataToEncode, embedding_model=embedding_model)
        else:
            # raise an error since this call has not been defined
            raise ValueError("Unsupported data type. Please provide a list, Series, string, or add unimplemented data type call.")

        # as instructed my "interface", return a torch tensor
        return np.array(embedded_list)

    @staticmethod
    def _make_openai_embedding_call(text:Any, embedding_model:str="text-embedding-ada-002"):
        '''this method is responsible for making the openai embeddings call. it takes in a single'''
        text = text.replace("\n", " ")
        return openai.Embedding.create(input = [text], model=embedding_model)['data'][0]['embedding']