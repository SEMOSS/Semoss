from typing import List, Dict, Union, Optional, Any
from .faiss_client import FAISSSearcher
from gaas_gpt_model import ModelEngine


class FAISSDatabase:
    """
    This the primary class to store all the FAISSSearcher for a given faiss database
    """

    def __init__(
        self,
        tokenizer,
        distance_method: str,
        embedder_engine_id: Optional[str] = None,
        keyword_engine_id: Optional[str] = None,
        searchers: list = [],
        model_engine_class: Any = ModelEngine,
        embedder_engine: ModelEngine = None,
        keyword_engine: ModelEngine = None,
    ) -> None:
        """
        Create an instance of FAISSDatabase
        """
        # first we have to determine what tokenizer we need
        self.tokenizer = tokenizer

        # set the embedder class so it can be used when new searchers/indexClasses are added
        if embedder_engine is not None:
            self.embeddings_engine = embedder_engine
        else:
            self.embeddings_engine = model_engine_class(engine_id=embedder_engine_id)

        if keyword_engine_id != None and keyword_engine_id != "":
            self.keyword_engine = model_engine_class(engine_id=keyword_engine_id)
        else:
            self.keyword_engine = None

        # what type of similarity search are we performing
        self.metric_type_is_cosine_similarity = False
        if distance_method.lower().find("cosine") > -1:
            self.metric_type_is_cosine_similarity = True

        # register all the searchers passed in
        self.searchers = {
            searcher: FAISSSearcher(
                embeddings_engine=self.embeddings_engine,
                keywords_engine=self.keyword_engine,
                tokenizer=self.tokenizer,
                metric_type_is_cosine_similarity=self.metric_type_is_cosine_similarity,
            )
            for searcher in searchers
        }

    def searcher_exists(self, searcher_name: str) -> bool:
        """
        Check if the searcher passed in exists

        Args:
            searcher_name(`str`):
              The name of the searcher to check.

        Returns:
            bool: True if exists, False otherwise
        """
        if searcher_name in self.searchers:
            return True

        return False

    def list_all_records(self) -> List[dict]:
        """
        Get the list of all the records across the searchers
        """
        all_values = []
        for searcher_name in self.searchers:
            all_values.extend(self.searchers[searcher_name].list_all_records())

        return all_values

    def create_searcher(self, searcher_name: str, **kwargs: Any) -> None:
        """
        Create a new searchers/indexClasses to which a set of documents will be added.

        Args:
            searcher_name(`str`):
                The name of the searcher to be added.

        Returns:
            `None`
        """
        if searcher_name in self.searchers.keys():
            raise ValueError("The searcher/table/class already exists")

        self.searchers[searcher_name] = FAISSSearcher(
            embeddings_engine=self.embeddings_engine,
            keywords_engine=self.keyword_engine,
            tokenizer=self.tokenizer,
            metric_type_is_cosine_similarity=self.metric_type_is_cosine_similarity,
            **kwargs
        )

    def delete_searcher(self, searcher_name: str) -> None:
        """
        Remove a searcher/indexClass from the the main database object

        Args:
            searcher_name(`str`):
                The name of the searcher to be removed from memory after the files have been deleted

        Returns:
            `None`
        """
        del self.searchers[searcher_name]

    def nearestNeighbor(
        self,
        indexClasses: List[str],
        question: str,
        filter: Optional[str] = None,
        results: Optional[int] = 5,
        columns_to_return: Optional[List[str]] = None,
        return_threshold: Optional[Union[int, float]] = 1000,
        ascending: Optional[bool] = None,
        total_results: Optional[int] = 10,  # this is used for reranking
        insight_id: Optional[str] = None,
    ) -> List[Dict]:
        '''
        Given a set of Index Classes, find the closest match(es) using FAISSearcher.nearestNeighbor across all index classes.

        Args:
            indexClasses(`List[str]`):
                A list of string defining the index classes to search in the database
            question(`str`):
                The string you are trying to match against the embedded documents
            filter(`str`):
                A SQL filter to find the appropriate indexes before executing the semantic search
            results(`Optional[int]`, *optional*):
                The number of matches under the threshold that will be returned
            columns_to_return(`List[str]`):
                A list of column names that will be sent back in the return payload.
                Example:
                # Given the following dataset
                >>> dataset
                Dataset({
                    features: ['doc_index', 'content', 'tokens', 'url'],
                    num_rows: 902
                })

                # if columns_to_return = None, then all four columns will be returned

                # if columns_to_return = ['doc_index']

                >>> FAISSearcher.nearestNeighbor(
                ...     question = 'Sample',
                ...     columns_to_return = ['doc_index'],
                ...     results = 1
                ... )
                [{'Score':0.23, "doc_index":"<theDocIndexThatMathced"}]
            return_threshold(`Optional[Union[int,float]]`):
                A numerical value that specifies what Score should be less than.
            ascending(`Optional[bool]`):
                A boolean flag to return results in ascending order or not. Default is True
            insight_id(`Optional[str]`):
                The unique identifier of the insight from which the call is being made
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
            if indexClass in self.searchers:
                # perform the nn search in the index class
                index_class_output = self.searchers[indexClass].nearestNeighbor(
                    question=question,
                    filter=filter,
                    results=results,
                    columns_to_return=columns_to_return,
                    return_threshold=return_threshold,
                    ascending=ascending,
                    total_results=total_results,
                    insight_id=insight_id,
                )

                # add the index class to the return payload for every object so the end user knows where the results are coming from
                if len(indexClasses) > 1:
                    index_class_output = [
                        {**output, "indexClass": indexClass}
                        for output in index_class_output
                    ]

                index_outputs.extend(index_class_output)

        # sort the total output and retrun the specified limit
        if len(index_outputs) > 0:
            index_outputs = sorted(
                index_outputs, key=lambda x: x["Score"], reverse=not ascending
            )[:results]

        return index_outputs
