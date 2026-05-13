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
        searchers: List = [],
        model_engine_class: Any = ModelEngine,
        embedder_engine: ModelEngine = None,
        keyword_engine: ModelEngine = None,
        enable_hybrid_search: bool = True,
    ) -> None:
        """
        Create an instance of FAISSDatabase with hybrid search support
        """
        self.tokenizer = tokenizer
        self.enable_hybrid_search = enable_hybrid_search

        # Setting up embedding engine
        if embedder_engine is not None:
            self.embeddings_engine = embedder_engine
        else:
            self.embeddings_engine = model_engine_class(engine_id=embedder_engine_id)

        # Setting up keyword engine
        if keyword_engine_id is not None and keyword_engine_id != "":
            self.keyword_engine = model_engine_class(engine_id=keyword_engine_id)
        else:
            self.keyword_engine = None

        # Determine similarity metric
        self.metric_type_is_cosine_similarity = False
        if distance_method.lower().find("cosine") > -1:
            self.metric_type_is_cosine_similarity = True
        # Determine default sort direction
        self.default_sort_direction = (
            False if self.metric_type_is_cosine_similarity else True
        )

        # searchers with hybrid search capability
        self.searchers = {
            searcher: FAISSSearcher(
                embeddings_engine=self.embeddings_engine,
                keywords_engine=self.keyword_engine,
                tokenizer=self.tokenizer,
                metric_type_is_cosine_similarity=self.metric_type_is_cosine_similarity,
                default_sort_direction=self.default_sort_direction,
                enable_hybrid_search=self.enable_hybrid_search,
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

    def create_searcher(
        self, searcher_name: str, base_path: str = None, **kwargs: Any
    ) -> None:
        """
        Create a new searcher with hybrid search capabilities
        """
        if searcher_name in self.searchers.keys():
            raise ValueError("The searcher/table/class already exists")

        self.searchers[searcher_name] = FAISSSearcher(
            embeddings_engine=self.embeddings_engine,
            keywords_engine=self.keyword_engine,
            tokenizer=self.tokenizer,
            metric_type_is_cosine_similarity=self.metric_type_is_cosine_similarity,
            default_sort_direction=self.default_sort_direction,
            enable_hybrid_search=self.enable_hybrid_search,
            base_path=base_path,
            **kwargs,
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

    def rebuild_bm25_indexes(self, indexClasses: List[str] = None) -> Dict[str, bool]:
        """
        Rebuild BM25 indexes for specified searchers
        """
        if not self.enable_hybrid_search:
            return {}

        if indexClasses is None:
            indexClasses = list(self.searchers.keys())

        results = {}
        for indexClass in indexClasses:
            if indexClass in self.searchers:
                searcher = self.searchers[indexClass]
                try:
                    if searcher.ds is not None and "Content" in searcher.ds.columns:
                        searcher.bm25_searcher.build_bm25_index(
                            list(searcher.ds["Content"])
                        )
                        results[indexClass] = True
                    else:
                        results[indexClass] = False
                except Exception as e:
                    print(f"Failed to rebuild BM25 index for {indexClass}: {e}")
                    results[indexClass] = False

        return results

    def get_search_statistics(self, indexClasses: List[str]) -> Dict[str, Any]:
        """
        Get statistics about the search indexes including BM25 status
        """
        stats = {
            "total_searchers": len(self.searchers),
            "hybrid_enabled": self.enable_hybrid_search,
            "searcher_details": {},
        }

        for indexClass in indexClasses:
            if indexClass in self.searchers:
                searcher = self.searchers[indexClass]
                stats["searcher_details"][indexClass] = {
                    "has_dataset": searcher.ds is not None,
                    "dataset_size": len(searcher.ds) if searcher.ds else 0,
                    "has_vectors": searcher.encoded_vectors is not None,
                    "vector_dimensions": searcher.vector_dimensions,
                    "has_bm25_index": searcher.bm25_index is not None,
                    "bm25_corpus_size": (
                        len(searcher.bm25_corpus) if searcher.bm25_corpus else 0
                    ),
                }

        return stats

    def nearestNeighbor(
        self,
        indexClasses: List[str],
        question: str,
        filter: Optional[str] = None,
        limit: Optional[int] = 5,
        columns_to_return: Optional[List[str]] = None,
        return_threshold: Optional[Union[int, float]] = 1000,
        total_limit: Optional[int] = 10,
        use_hybrid_search: Optional[bool] = None,
        vector_weight: Optional[Union[int, float]] = None,
        bm25_weight: Optional[Union[int, float]] = None,
        insight_id: Optional[str] = None,
    ) -> List[Dict]:
        """
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
            insight_id(`Optional[str]`):
                The unique identifier of the insight from which the call is being made
            use_hybrid_search(`Optional[bool]`):
                A boolean flag to enable or disable hybrid search. If None, the value of self.enable_hybrid_search will be used. True means both vector and BM25 searches will be performed.
            Return:
                `List[Dict]` consisting of Score and columns

        Example:
            >>> ag4ariA.nearestNeighbor(
            ...     indexClasses = ['default','secondClass'],
            ...     question=""How is the president chosen"",
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
        """
        assert isinstance(indexClasses, list)

        index_outputs = []
        for indexClass in indexClasses:
            if indexClass in self.searchers:

                if use_hybrid_search is None:
                    use_hybrid_search = self.enable_hybrid_search

                index_class_output = self.searchers[indexClass].nearestNeighbor(
                    question=question,
                    filter=filter,
                    limit=limit,
                    columns_to_return=columns_to_return,
                    return_threshold=return_threshold,
                    total_limit=total_limit,
                    use_hybrid_search=use_hybrid_search,
                    vector_weight=vector_weight,
                    bm25_weight=bm25_weight,
                    insight_id=insight_id,
                )

                if len(indexClasses) > 1:
                    index_class_output = [
                        {**output, "indexClass": indexClass}
                        for output in index_class_output
                    ]

                index_outputs.extend(index_class_output)

        # Sort results based on the search mode
        if len(index_outputs) > 0:
            if use_hybrid_search and any(
                "Weighted_RRF_Score" in result for result in index_outputs
            ):
                # Sort by Weighted_RRF_Score for hybrid results
                index_outputs = sorted(
                    index_outputs,
                    key=lambda x: x.get("Weighted_RRF_Score", 0),
                    reverse=True,
                )[:limit]
            else:
                index_outputs = sorted(
                    index_outputs,
                    key=lambda x: x["Score"],
                    reverse=(not self.default_sort_direction),
                )[:limit]

        return index_outputs
