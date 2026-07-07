from typing import Any, Dict, List, Optional, Union

from gaas_gpt_model import ModelEngine

from .qdrant_searcher import QdrantSearcher


class QdrantDatabase:

    def __init__(
        self,
        tokenizer,
        distance_method: str,
        embedder_engine_id: Optional[str] = None,
        keyword_engine_id: Optional[str] = None,
        location: str = ":memory:",
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        prefer_grpc: bool = False,
        quantization: str = "none",
        hnsw_m: Optional[int] = None,
        hnsw_ef_construct: Optional[int] = None,
        on_disk_payload: bool = False,
        enable_hybrid_search: bool = False,
        sparse_model_name: str = "Qdrant/bm25",
        fusion: str = "rrf",
        indexed_fields: Optional[List[Dict[str, str]]] = None,
        searchers: Optional[List[str]] = None,
        model_engine_class: Any = ModelEngine,
        embedder_engine: ModelEngine = None,
        keyword_engine: ModelEngine = None,
    ) -> None:
        from qdrant_client import QdrantClient

        self.tokenizer = tokenizer
        self.location = location or ":memory:"
        self.url = url
        self.api_key = api_key
        self.prefer_grpc = bool(prefer_grpc)
        self.quantization = (quantization or "none").lower()
        self.hnsw_m = hnsw_m
        self.hnsw_ef_construct = hnsw_ef_construct
        self.on_disk_payload = bool(on_disk_payload)
        self.enable_hybrid_search = bool(enable_hybrid_search)
        self.sparse_model_name = sparse_model_name or "Qdrant/bm25"
        self.fusion = (fusion or "rrf").lower()
        self.indexed_fields = list(indexed_fields) if indexed_fields else [
            {"field": "Source", "schema": "keyword"}
        ]

        if embedder_engine is not None:
            self.embeddings_engine = embedder_engine
        else:
            self.embeddings_engine = model_engine_class(engine_id=embedder_engine_id)

        if keyword_engine_id is not None and keyword_engine_id != "":
            self.keyword_engine = model_engine_class(engine_id=keyword_engine_id)
        else:
            self.keyword_engine = None

        normalized = (distance_method or "Cosine Similarity").lower()
        if "cosine" in normalized:
            self.distance = "Cosine"
            self.metric_type_is_cosine_similarity = True
        elif "dot" in normalized or "inner" in normalized:
            self.distance = "Dot"
            self.metric_type_is_cosine_similarity = False
        elif "manhattan" in normalized:
            self.distance = "Manhattan"
            self.metric_type_is_cosine_similarity = False
        else:
            self.distance = "Euclid"
            self.metric_type_is_cosine_similarity = False

        self.default_sort_direction = not self.metric_type_is_cosine_similarity

        if self.url:
            client_kwargs: Dict[str, Any] = {"url": self.url, "prefer_grpc": self.prefer_grpc}
            if self.api_key:
                client_kwargs["api_key"] = self.api_key
            self.client = QdrantClient(**client_kwargs)
            self.is_local = False
        elif self.location == ":memory:":
            self.client = QdrantClient(":memory:")
            self.is_local = True
        else:
            self.client = QdrantClient(path=self.location)
            self.is_local = True

        if self.enable_hybrid_search and self.is_local:
            import warnings as _warnings
            _warnings.warn(
                "QdrantLocal (in-memory or path=…) provides only partial support "
                "for hybrid search: BM25 IDF is approximated and some fusion paths "
                "are stubbed. Point QdrantDatabase(url='http://host:6333') at a real "
                "Qdrant server for production hybrid search.",
                stacklevel=2,
            )

        self.searchers: Dict[str, QdrantSearcher] = {}
        if searchers:
            for name in searchers:
                self.create_searcher(name)

    def searcher_exists(self, searcher_name: str) -> bool:
        return searcher_name in self.searchers

    def list_all_records(self) -> List[dict]:
        all_values: List[dict] = []
        for name in self.searchers:
            all_values.extend(self.searchers[name].list_all_records())
        return all_values

    def create_searcher(
        self,
        searcher_name: str,
        base_path: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        if searcher_name in self.searchers:
            raise ValueError("The searcher/table/class already exists")
        self.searchers[searcher_name] = QdrantSearcher(
            client=self.client,
            collection_name=searcher_name,
            embeddings_engine=self.embeddings_engine,
            keyword_engine=self.keyword_engine,
            tokenizer=self.tokenizer,
            distance=self.distance,
            metric_type_is_cosine_similarity=self.metric_type_is_cosine_similarity,
            default_sort_direction=self.default_sort_direction,
            quantization=self.quantization,
            hnsw_m=self.hnsw_m,
            hnsw_ef_construct=self.hnsw_ef_construct,
            on_disk_payload=self.on_disk_payload,
            enable_hybrid_search=self.enable_hybrid_search,
            sparse_model_name=self.sparse_model_name,
            fusion=self.fusion,
            indexed_fields=self.indexed_fields,
            is_local=self.is_local,
            base_path=base_path,
            **kwargs,
        )

    def delete_searcher(self, searcher_name: str) -> None:
        if searcher_name not in self.searchers:
            return
        try:
            self.searchers[searcher_name].drop_collection()
        finally:
            del self.searchers[searcher_name]

    def nearestNeighbor(
        self,
        indexClasses: List[str],
        question: str,
        limit: Optional[int] = 5,
        columns_to_return: Optional[List[str]] = None,
        score_threshold: Optional[Union[int, float]] = None,
        qdrant_filter: Optional[Dict[str, Any]] = None,
        insight_id: Optional[str] = None,
        use_hybrid_search: Optional[bool] = None,
        hybrid_prefetch_limit: Optional[int] = None,
    ) -> List[Dict]:
        assert isinstance(indexClasses, list)
        index_outputs: List[Dict[str, Any]] = []
        for indexClass in indexClasses:
            if indexClass not in self.searchers:
                continue
            results = self.searchers[indexClass].nearestNeighbor(
                question=question,
                limit=limit,
                columns_to_return=columns_to_return,
                score_threshold=score_threshold,
                qdrant_filter=qdrant_filter,
                insight_id=insight_id,
                use_hybrid_search=use_hybrid_search,
                hybrid_prefetch_limit=hybrid_prefetch_limit,
            )
            if len(indexClasses) > 1:
                results = [{**r, "indexClass": indexClass} for r in results]
            index_outputs.extend(results)

        if not index_outputs:
            return []

        index_outputs = sorted(
            index_outputs,
            key=lambda x: x.get("Score", 0),
            reverse=(not self.default_sort_direction),
        )
        if limit is not None:
            index_outputs = index_outputs[:limit]
        return index_outputs
