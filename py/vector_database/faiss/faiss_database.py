from typing import List, Dict, Union, Optional, Any
from .faiss_client import FAISSSearcher

class FAISSDatabase():
    '''
    This the primary class to store all the FAISSSearcher for a given faiss database
    '''

    def __init__(
        self, 
        searchers: list = [], 
        encoder_class = None
    ) -> None:
        '''
        Create an instance of FAISSDatabase
        '''
        # set the encoder class so it can be used when new searchers/indexClasses are added
        self.encoder_class = encoder_class

        # register all the searchers passed in
        self.searchers = {searcher:FAISSSearcher(encoder_class = self.encoder_class) for searcher in searchers}

    def create_searcher(
        self, 
        searcher_name: str, 
        **kwargs: Any
    ) -> None:
        '''
        Create a new searchers/indexClasses to which a set of documents will be added.

        Args:
            searcher_name(`str`):
                The name of the searcher to be added.
        
        Returns:
            `None`
        '''
        if (searcher_name in self.searchers.keys()):
            raise ValueError("The searcher/table/class already exists")
        
        self.searchers[searcher_name] = FAISSSearcher(encoder_class = self.encoder_class, **kwargs)

    def delete_searcher(
        self, 
        searcher_name: str
    ) -> None:
        '''
        Remove a searcher/indexClass from the the main database object

        Args:
            searcher_name(`str`):
                The name of the searcher to be removed from memory after the files have been deleted
        
        Returns:
            `None`
        '''
        del self.searchers[searcher_name]