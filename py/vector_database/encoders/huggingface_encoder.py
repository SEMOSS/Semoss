from transformers import AutoTokenizer, AutoModel
from sentence_transformers import SentenceTransformer, util
from typing import Any, List
from .encoder_interface import EncoderInterface
import pandas
import numpy as np

class HuggingFaceEncoder(EncoderInterface):

    def __init__(self, embedding_model:str = None, api_key:str = None) -> None:
        self.embedding_model = embedding_model
        self.api_key = api_key
        try:
            #tokenizer = AutoTokenizer.from_pretrained(embedding_model)
            #model = AutoModel.from_pretrained(embedding_model)

            #tokenizer = AutoTokenizer.from_pretrained(embedding_model)
            self.model = SentenceTransformer(embedding_model)
        except:
            # TODO might need to double check the wording of this error
            raise ValueError("Please make sure the embedding model is an intance of sentence transforers")

    def get_embeddings(self, dataToEncode:Any, embedding_model:str = None, api_key:str = None) -> np.array:
        try:
            #tokenizer = AutoTokenizer.from_pretrained(embedding_model)
            #model = AutoModel.from_pretrained(embedding_model)

            #tokenizer = AutoTokenizer.from_pretrained(embedding_model)
            model = SentenceTransformer(embedding_model)
        except:
            # TODO might need to double check the wording of this error
            raise ValueError("Please make sure the embedding model is an intance of sentence transforers")

        # Determine what object was bassed in so we can pre-configure it before making the call
        if isinstance(dataToEncode, list) or isinstance(dataToEncode, str):
            #embedded_tensor = self._make_transformer_encoding_call(text_list = dataToEncode, tokenizer = tokenizer, model = model)
            embedded_tensor = self.model.encode(dataToEncode)
        elif isinstance(dataToEncode, pandas.core.series.Series):
            #embedded_tensor = self._make_transformer_encoding_call(text_list = list(dataToEncode), tokenizer = tokenizer, model = model)
            embedded_tensor = self.model.encode(dataToEncode.tolist())
        # elif isinstance(dataToEncode, str):
        #     print(dataToEncode)
        #     #embedded_tensor = self._make_transformer_encoding_call(text_list = [dataToEncode], tokenizer = tokenizer, model = model)
        else:
            # raise an error since this call has not been defined
            raise ValueError("Unsupported data type. Please provide a list, Series, string, or add unimplemented data type call.")
        
        return embedded_tensor
    
    def _make_transformer_encoding_call(self, text_list:List, tokenizer:Any, model:Any):
        
        encoded_input = tokenizer(
            text_list, padding=True, truncation=True, return_tensors="pt"
        )

        # need to make sure the model and vector are on the same device
        encoded_input = {k: v.to(self.device) for k, v in encoded_input.items()}
        model.to(self.device)

        model_output = model(**encoded_input)
        return model_output
    