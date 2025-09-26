from typing import List, Dict, Union, Optional, Any

from .faiss_client import FAISSSearcher
from gaas_gpt_model import ModelEngine

class FAISSDatabase:
    """
    This is the primary class to store all the FAISSSearcher for a given faiss database,
    now supports per-searcher metadata table indexed by ID.
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
        self.tokenizer = tokenizer
        self.embeddings_engine = embedder_engine or model_engine_class(engine_id=embedder_engine_id)
        self.keyword_engine = model_engine_class(engine_id=keyword_engine_id) if keyword_engine_id else None
        self.metric_type_is_cosine_similarity = "cosine" in distance_method.lower()
        self.searchers = {
            searcher: FAISSSearcher(
                embeddings_engine=self.embeddings_engine,
                keywords_engine=self.keyword_engine,
                tokenizer=self.tokenizer,
                metric_type_is_cosine_similarity=self.metric_type_is_cosine_similarity,
            ) for searcher in searchers
        }
        self.metadata_tables: Dict[str, Dict[str, dict]] = {}

    def searcher_exists(self, searcher_name: str) -> bool:
        return searcher_name in self.searchers

    def list_all_records(self) -> List[dict]:
        all_values = []
        for searcher_name in self.searchers:
            all_values.extend(self.searchers[searcher_name].list_all_records())
        return all_values

    def create_searcher(self, searcher_name: str, **kwargs: Any) -> None:
        if searcher_name in self.searchers:
            raise ValueError("The searcher/table/class already exists")
        self.searchers[searcher_name] = FAISSSearcher(
            embeddings_engine=self.embeddings_engine,
            keywords_engine=self.keyword_engine,
            tokenizer=self.tokenizer,
            metric_type_is_cosine_similarity=self.metric_type_is_cosine_similarity,
            **kwargs
        )

    def delete_searcher(self, searcher_name: str) -> None:
        if searcher_name in self.searchers:
            del self.searchers[searcher_name]
        if searcher_name in self.metadata_tables:
            del self.metadata_tables[searcher_name]

    def add_metadata(self, searcher_name: str, metadata_map: Dict[str, dict]):
        """
        Add metadata rows to the index class (searcher), indexed by unique 'id'.
        metadata_map: Dict[id, metadata_dict]
        """
        if searcher_name not in self.metadata_tables:
            self.metadata_tables[searcher_name] = {}
        for id_key, meta_row in metadata_map.items():
            self.metadata_tables[searcher_name][id_key] = meta_row
        if searcher_name in self.searchers:
            self.searchers[searcher_name].add_metadata(self.metadata_tables[searcher_name])

    def nearestNeighbor(
        self,
        indexClasses: List[str],
        question: str,
        filter: Optional[str] = None,
        results: Optional[int] = 5,
        columns_to_return: Optional[List[str]] = None,
        return_threshold: Optional[Union[int, float]] = 1000,
        ascending: Optional[bool] = None,
        total_results: Optional[int] = 10,
        insight_id: Optional[str] = None,
    ) -> List[Dict]:
        """
        Returns merged vector + metadata results for each indexClass.
        """
        assert isinstance(indexClasses, list)
        index_outputs = []
        for indexClass in indexClasses:
            if indexClass in self.searchers:
                output = self.searchers[indexClass].nearestNeighbor(
                    question=question, filter=filter, results=results,
                    columns_to_return=columns_to_return,
                    return_threshold=return_threshold,
                    ascending=ascending, total_results=total_results,
                    insight_id=insight_id
                )
                if len(indexClasses) > 1:
                    output = [{**row, "indexClass": indexClass} for row in output]
                # Attach metadata by id
                meta_tbl = self.metadata_tables.get(indexClass, {})
                for row in output:
                    row_id = row.get('ID') or row.get('id')
                    if meta_tbl and row_id in meta_tbl:
                        row["metadata"] = meta_tbl[row_id]
                index_outputs.extend(output)
        if index_outputs:
            index_outputs = sorted(index_outputs, key=lambda x: x["Score"], reverse=not ascending)[:results]
        return index_outputs

    def add_metadata_table(self, searcher_name: str):
        """
        Create an empty metadata table for the searcher, if not exists.
        """
        if searcher_name not in self.metadata_tables:
            self.metadata_tables[searcher_name] = {}

    def metadata_query(self, searcher_name: str, filter_func) -> List[dict]:
        """
        Query metadata rows using a filter function.
        filter_func: Callable[[dict], bool]
        """
        tbl = self.metadata_tables.get(searcher_name, {})
        return [row for row in tbl.values() if filter_func(row)]
