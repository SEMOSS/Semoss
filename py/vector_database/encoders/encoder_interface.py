from abc import ABC, abstractmethod
from typing import Any
import torch # are we going to return a torch tensor for all embeddings calls? is this the standard
from datasets import load_dataset, Dataset

class EncoderInterface(ABC):

    # TODO need to check with Kunal how we will serve this to the same gpu
    device = 'cuda' if torch.cuda.is_available() else 'cpu'

    '''All of the methods listed here will result in TypeError during runtime if the are not implemented'''
    @abstractmethod
    def get_embeddings(self, dataToEncode:Any, embedding_model:str = None, api_key:str = None) -> torch.Tensor:
        pass

    @staticmethod
    def cls_pooling(model_output):
        return model_output.last_hidden_state[:, 0]