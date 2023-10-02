from .faiss_client import FAISSSearcher
from ..encoders.huggingface_encoder import HuggingFaceEncoder

class FAISSDatabase():
    '''this the primary class for a faiss database'''

    def __init__(self, searchers:list = [], encoder_class = None):
        self.encoder_class = encoder_class
        self.searchers = {searcher:FAISSSearcher(encoder_class = self.encoder_class) for searcher in searchers}

    # I think thats all I need?
    def create_searcher(self, searcher_name, **kwargs):
        if (searcher_name in self.searchers.keys()):
            raise ValueError("The searcher/table/class already exists")
        
        self.searchers[searcher_name] = FAISSSearcher(encoder_class = self.encoder_class, **kwargs)

    def delete_searcher(self, searcher_name):
        del self.searchers[searcher_name]