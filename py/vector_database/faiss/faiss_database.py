from typing import List, Dict, Union, Optional, Any
from .faiss_client import FAISSSearcher
from gaas_gpt_model import ModelEngine
from genai_client import get_tokenizer

class FAISSDatabase():
    '''
    This the primary class to store all the FAISSSearcher for a given faiss database
    '''

    def __init__(
        self, 
        embedder_engine_id: str,
        tokenizer,
        keyword_engine_id: str,
        distance_method: str,
        searchers: list = [],
        embedder_engine: ModelEngine = None,
        keyword_engine: ModelEngine = None,
    ) -> None:
        '''
        Create an instance of FAISSDatabase
        '''
        # first we have to determine what tokenizer we need
        self.tokenizer = tokenizer

        # set the embedder class so it can be used when new searchers/indexClasses are added
        if embedder_engine is not None:
            self.embeddings_engine = embedder_engine
        else:
            self.embeddings_engine = ModelEngine(engine_id = embedder_engine_id)
        
        if (keyword_engine_id != None and keyword_engine_id != ''):
            self.keyword_engine = ModelEngine(engine_id = keyword_engine_id)
        else:
            self.keyword_engine = keyword_engine
        
        # what type of similarity search are we performing
        self.metric_type_is_cosine_similarity = False
        if distance_method.lower().find('cosine') > -1:
            self.metric_type_is_cosine_similarity = True
        
        # register all the searchers passed in
        self.searchers = {
            searcher:FAISSSearcher(
                embeddings_engine = self.embeddings_engine,
                keywords_engine = self.keyword_engine,
                tokenizer = self.tokenizer, 
                metric_type_is_cosine_similarity = self.metric_type_is_cosine_similarity
            ) 
            for searcher in searchers
        }

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
        
        self.searchers[searcher_name] = FAISSSearcher(
            embeddings_engine = self.embeddings_engine,
            keywords_engine = self.keyword_engine,
            tokenizer = self.tokenizer, 
            metric_type_is_cosine_similarity = self.metric_type_is_cosine_similarity,
            **kwargs
        )

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

    def nearestNeighbor(
        self,
        indexClasses: List[str],
        results: Optional[Union[int, None]] = 5,
        ascending: Optional[Union[bool, None]] = True,
        **kwargs
    ) -> List[Dict]:
        '''
        Given a set of Index Classes, find the closest match(es) using FAISSearcher.nearestNeighbor across all index classes.

        Args:
            indexClasses(`List[str]`):
                A list of string defining the index classes to search in the database
            results(`Optional[Union[int, None]]`):
                The number of matches under the threshold that will be returned. This same limit will be used for every index class search in case all of the top results come from a singular class
            ascending(`Optional[Union[bool, None]]`):
                 A boolean flag to return results in ascending order or not. Default is True.
        
        Return:
            `List[Dict]` consisting of Score and columns

        Example:
            >>> ag4ariA.nearestNeighbor(
            ...     indexClasses = ['default','secondClass'],
            ...     question="""How is the president chosen""",
            ...     results = 2
            ... )
            [{'Score': 0.7656829357147217,
            'Source': 'constitution.pdf',
            'Divider': 6,
            'Part': 1,
            'Content': ' He shall hold his Office during the Term of four Years , and , together with the Vice Presi - dent , chosen for the same Term , be elected , as follows : Each State shall appoint , in such Manner as the Legislature thereof may direct , a Number of Electors , equal to the whole Number of Senators and Representatives to which the State may be entitled in the Congress : but no Senator or Representative , or Person holding an Office of Trust or Prof - it under the United States , shall be appointed an Elector .',
            'indexClass': 'default'},
            {'Score': 0.7656829357147217,
            'Source': 'constitution2.pdf',
            'Divider': 6,
            'Part': 1,
            'Content': ' He shall hold his Office during the Term of four Years , and , together with the Vice Presi - dent , chosen for the same Term , be elected , as follows : Each State shall appoint , in such Manner as the Legislature thereof may direct , a Number of Electors , equal to the whole Number of Senators and Representatives to which the State may be entitled in the Congress : but no Senator or Representative , or Person holding an Office of Trust or Prof - it under the United States , shall be appointed an Elector .',
            'indexClass': 'secondClass'}]
        '''
        # make sure a list was passed in so we dont have runtime error later
        assert isinstance(indexClasses, list)

        index_outputs = []
        for indexClass in indexClasses:
            # perform the nn search in the index class
            index_class_output = self.searchers[indexClass].nearestNeighbor(
                results = results, 
                ascending = ascending, 
                **kwargs
            )

            # add the index class to the return payload for every object so the end user knows where the results are coming from
            index_class_output = [{**output, "indexClass": indexClass} for output in index_class_output]

            index_outputs.extend(
                index_class_output
            )
        
        # sort the total output and retrun the specified limit
        index_outputs = sorted(index_outputs, key=lambda x: x['Score'], reverse= not ascending)[:results]
        
        return index_outputs
    