import logging
import os
import uuid
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from ..constants import ENCODING_OPTIONS

logger = logging.getLogger(__name__)

_NAMESPACE = uuid.UUID("4f7c0c3a-c0f4-5b25-9e7e-1a5d4d2c8e7a")

DENSE_VECTOR_NAME = "dense"
SPARSE_VECTOR_NAME = "sparse"
DEFAULT_SPARSE_MODEL = "Qdrant/bm25"

_PAYLOAD_SCHEMA_MAP = {
    "keyword": "KEYWORD",
    "integer": "INTEGER",
    "float": "FLOAT",
    "bool": "BOOL",
    "boolean": "BOOL",
    "geo": "GEO",
    "text": "TEXT",
    "datetime": "DATETIME",
    "uuid": "UUID",
}


class QdrantSearcher:

    def __init__(
        self,
        client,
        collection_name: str,
        embeddings_engine,
        tokenizer,
        distance: str = "Cosine",
        metric_type_is_cosine_similarity: bool = True,
        default_sort_direction: bool = False,
        keyword_engine=None,
        quantization: str = "none",
        hnsw_m: Optional[int] = None,
        hnsw_ef_construct: Optional[int] = None,
        on_disk_payload: bool = False,
        base_path: Optional[str] = None,
        enable_hybrid_search: bool = False,
        sparse_model_name: str = DEFAULT_SPARSE_MODEL,
        fusion: str = "rrf",
        indexed_fields: Optional[List[Dict[str, str]]] = None,
        is_local: bool = True,
        **kwargs: Any,
    ) -> None:
        from qdrant_client import models

        self._models = models
        self.client = client
        self.collection_name = collection_name
        self.embeddings_engine = embeddings_engine
        self.keyword_engine = keyword_engine
        self.tokenizer = tokenizer
        self.distance_name = distance
        self.metric_type_is_cosine_similarity = metric_type_is_cosine_similarity
        self.default_sort_direction = default_sort_direction
        self.quantization = (quantization or "none").lower()
        self.hnsw_m = hnsw_m
        self.hnsw_ef_construct = hnsw_ef_construct
        self.on_disk_payload = on_disk_payload
        self.base_path = base_path
        self.enable_hybrid_search = bool(enable_hybrid_search)
        self.sparse_model_name = sparse_model_name or DEFAULT_SPARSE_MODEL
        self.fusion = (fusion or "rrf").lower()
        self.indexed_fields = list(indexed_fields) if indexed_fields else [
            {"field": "Source", "schema": "keyword"}
        ]
        self.is_local = bool(is_local)
        self.vector_size: Optional[int] = None
        self._sources: set = set()
        self._collection_ready = False
        self._sparse_encoder = None
        self._is_hybrid_collection = False

    def _resolve_distance(self):
        m = self._models.Distance
        return {
            "Cosine": m.COSINE, "Dot": m.DOT, "Euclid": m.EUCLID, "Manhattan": m.MANHATTAN,
        }.get(self.distance_name, m.COSINE)

    def _quantization_config(self):
        if self.quantization == "scalar":
            return self._models.ScalarQuantization(
                scalar=self._models.ScalarQuantizationConfig(
                    type=self._models.ScalarType.INT8, always_ram=True))
        if self.quantization == "binary":
            return self._models.BinaryQuantization(
                binary=self._models.BinaryQuantizationConfig(always_ram=True))
        return None

    def _hnsw_config(self):
        if self.hnsw_m is None and self.hnsw_ef_construct is None:
            return None
        kwargs = {}
        if self.hnsw_m is not None: kwargs["m"] = int(self.hnsw_m)
        if self.hnsw_ef_construct is not None: kwargs["ef_construct"] = int(self.hnsw_ef_construct)
        return self._models.HnswConfigDiff(**kwargs)

    def _get_sparse_encoder(self):
        if self._sparse_encoder is not None:
            return self._sparse_encoder
        from fastembed import SparseTextEmbedding
        self._sparse_encoder = SparseTextEmbedding(model_name=self.sparse_model_name)
        return self._sparse_encoder

    def _encode_sparse_batch(self, texts: List[str]):
        encoder = self._get_sparse_encoder()
        return list(encoder.embed(texts))

    def _encode_sparse_query(self, text: str):
        encoder = self._get_sparse_encoder()
        return list(encoder.query_embed([text]))[0]

    def _to_sparse_vector(self, sparse_embedding):
        indices = sparse_embedding.indices.tolist() if hasattr(sparse_embedding.indices, "tolist") \
            else list(sparse_embedding.indices)
        values = sparse_embedding.values.tolist() if hasattr(sparse_embedding.values, "tolist") \
            else list(sparse_embedding.values)
        return self._models.SparseVector(indices=indices, values=values)

    def _ensure_collection(self, vector_size: int) -> None:
        if self._collection_ready:
            return
        self.vector_size = vector_size
        existing = {c.name for c in self.client.get_collections().collections}
        if self.collection_name not in existing:
            self._create_collection(vector_size)
        else:
            self._detect_existing_collection_mode()
        self._collection_ready = True
        self._refresh_sources_from_qdrant()

    def _create_collection(self, vector_size: int) -> None:
        dense_params = self._models.VectorParams(
            size=vector_size, distance=self._resolve_distance())

        if self.enable_hybrid_search:
            vectors_config = {DENSE_VECTOR_NAME: dense_params}
            sparse_vectors_config = {
                SPARSE_VECTOR_NAME: self._models.SparseVectorParams(
                    modifier=self._models.Modifier.IDF)
            }
        else:
            vectors_config = dense_params
            sparse_vectors_config = None

        create_kwargs: Dict[str, Any] = {
            "collection_name": self.collection_name,
            "vectors_config": vectors_config,
            "on_disk_payload": self.on_disk_payload,
        }
        if sparse_vectors_config is not None:
            create_kwargs["sparse_vectors_config"] = sparse_vectors_config
        quant = self._quantization_config()
        if quant is not None: create_kwargs["quantization_config"] = quant
        hnsw = self._hnsw_config()
        if hnsw is not None: create_kwargs["hnsw_config"] = hnsw

        self.client.create_collection(**create_kwargs)
        self._is_hybrid_collection = self.enable_hybrid_search
        self._ensure_payload_indexes()

    def _ensure_payload_indexes(self) -> None:
        for spec in self.indexed_fields:
            field = spec.get("field")
            if not field:
                continue
            schema_key = (spec.get("schema") or "keyword").lower()
            schema_enum = _PAYLOAD_SCHEMA_MAP.get(schema_key, "KEYWORD")
            try:
                field_schema = getattr(self._models.PayloadSchemaType, schema_enum)
                self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=field,
                    field_schema=field_schema,
                )
            except Exception as e:
                logger.debug(
                    "create_payload_index skipped for %s (%s): %s",
                    field, schema_key, e,
                )

    def _detect_existing_collection_mode(self) -> None:
        try:
            info = self.client.get_collection(self.collection_name)
            params = info.config.params
            sparse_cfg = getattr(params, "sparse_vectors", None)
            self._is_hybrid_collection = sparse_cfg is not None and SPARSE_VECTOR_NAME in sparse_cfg
        except Exception:
            self._is_hybrid_collection = self.enable_hybrid_search
        self._ensure_payload_indexes()

    def _refresh_sources_from_qdrant(self) -> None:
        try:
            offset = None
            sources: set = set()
            while True:
                points, offset = self.client.scroll(
                    collection_name=self.collection_name, limit=512,
                    with_payload=True, with_vectors=False, offset=offset)
                for p in points:
                    src = (p.payload or {}).get("Source")
                    if src: sources.add(src)
                if offset is None: break
            self._sources = sources
        except Exception:
            self._sources = set()

    def _read_csv(self, path: str) -> pd.DataFrame:
        last_err: Optional[Exception] = None
        for enc in ENCODING_OPTIONS:
            try: return pd.read_csv(path, encoding=enc)
            except UnicodeDecodeError as e: last_err = e
            except Exception as e: last_err = e; break
        if last_err is not None: raise last_err
        return pd.read_csv(path)

    def _embed_batch(self, texts: List[str], insight_id: Optional[str]) -> List[List[float]]:
        response = self.embeddings_engine.embeddings(strings_to_embed=list(texts), insight_id=insight_id)
        if isinstance(response, list):
            if not response: return []
            first = response[0]
            if isinstance(first, dict) and "response" in first:
                return first["response"]
            return response
        if isinstance(response, dict) and "response" in response:
            return response["response"]
        return response

    def _point_id_for(self, source: str, divider: Any, part: Any) -> str:
        key = f"{self.collection_name}|{source}|{divider}|{part}"
        return str(uuid.uuid5(_NAMESPACE, key))

    def _build_point_vector(self, dense_vec: List[float], sparse_embedding=None):
        if self._is_hybrid_collection and sparse_embedding is not None:
            return {
                DENSE_VECTOR_NAME: dense_vec,
                SPARSE_VECTOR_NAME: self._to_sparse_vector(sparse_embedding),
            }
        return dense_vec

    def addDocument(
        self,
        documentFileLocation: List[str],
        insight_id: Optional[str] = None,
        columns_to_index: Optional[List[str]] = None,
        columns_to_remove: Optional[List[str]] = None,
        batch_size: int = 64,
    ) -> Dict[str, Any]:
        columns_to_index = columns_to_index or ["Content"]
        created_documents: List[str] = []

        for csv_path in documentFileLocation:
            if not os.path.isfile(csv_path): continue
            df = self._read_csv(csv_path)
            if df.empty: continue
            if columns_to_remove:
                for col in columns_to_remove:
                    if col in df.columns: df = df.drop(columns=[col])

            text_col = columns_to_index[0] if columns_to_index[0] in df.columns else None
            if text_col is None and "Content" in df.columns: text_col = "Content"
            if text_col is None: continue

            source_default = os.path.basename(csv_path)
            if "Source" not in df.columns: df["Source"] = source_default

            payloads, ids, texts = [], [], []
            for _, row in df.iterrows():
                content = row[text_col]
                if pd.isna(content): continue
                content_str = str(content)
                src = str(row.get("Source", source_default))
                divider = row.get("Divider", 0)
                part = row.get("Part", 0)
                pid = self._point_id_for(src, divider, part)
                payload = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                ids.append(pid); texts.append(content_str); payloads.append(payload)

            if not texts: continue

            for start in range(0, len(texts), batch_size):
                batch_texts = texts[start:start + batch_size]
                batch_ids = ids[start:start + batch_size]
                batch_payloads = payloads[start:start + batch_size]
                vectors = self._embed_batch(batch_texts, insight_id)
                if not vectors: continue
                if self.vector_size is None: self._ensure_collection(len(vectors[0]))

                sparse_vectors = None
                if self._is_hybrid_collection:
                    sparse_vectors = self._encode_sparse_batch(batch_texts)

                points = []
                for idx, (pid, vec, payload) in enumerate(zip(batch_ids, vectors, batch_payloads)):
                    sparse_emb = sparse_vectors[idx] if sparse_vectors is not None else None
                    point_vec = self._build_point_vector(vec, sparse_emb)
                    points.append(self._models.PointStruct(id=pid, vector=point_vec, payload=payload))
                if self.is_local:
                    self.client.upsert(collection_name=self.collection_name, points=points)
                else:
                    self.client.upload_points(
                        collection_name=self.collection_name,
                        points=points,
                        batch_size=batch_size,
                        parallel=2,
                        wait=False,
                    )

                for payload in batch_payloads:
                    src = payload.get("Source")
                    if src: self._sources.add(str(src))
            created_documents.append(csv_path)

        return {"createdDocuments": created_documents}

    def datasetsLoaded(self) -> bool:
        if not self._collection_ready: return False
        try:
            info = self.client.get_collection(self.collection_name)
            return (info.points_count or 0) > 0
        except Exception:
            return False

    def list_documents(self) -> List[str]:
        if not self._collection_ready: self._refresh_sources_from_qdrant()
        return sorted(self._sources)

    def list_all_records(self) -> List[Dict[str, Any]]:
        if not self._collection_ready: return []
        results: List[Dict[str, Any]] = []
        offset = None
        while True:
            points, offset = self.client.scroll(
                collection_name=self.collection_name, limit=512,
                with_payload=True, with_vectors=False, offset=offset)
            for p in points:
                record = dict(p.payload or {})
                record["id"] = p.id
                results.append(record)
            if offset is None: break
        return results

    def removeDocument(self, source: str) -> int:
        if not self._collection_ready: return 0
        flt = self._models.Filter(must=[
            self._models.FieldCondition(key="Source", match=self._models.MatchValue(value=source))])
        result = self.client.delete(
            collection_name=self.collection_name,
            points_selector=self._models.FilterSelector(filter=flt))
        self._sources.discard(source)
        return getattr(result, "operation_id", 0) or 0

    def removePoints(self, point_ids: List[str]) -> int:
        if not self._collection_ready or not point_ids: return 0
        self.client.delete(
            collection_name=self.collection_name,
            points_selector=self._models.PointIdsList(points=point_ids))
        return len(point_ids)

    def drop_collection(self) -> None:
        try: self.client.delete_collection(self.collection_name)
        except Exception: pass
        self._collection_ready = False
        self._is_hybrid_collection = False
        self._sources.clear()

    def _build_filter(self, qdrant_filter: Optional[Dict[str, Any]]):
        if not qdrant_filter:
            return None
        try:
            return self._models.Filter(**qdrant_filter)
        except Exception as e:
            logger.warning(
                "Ignoring malformed qdrant_filter on %s: %s",
                self.collection_name, e,
            )
            return None

    def _embed_query(self, question: str, insight_id: Optional[str]) -> Optional[List[float]]:
        vectors = self._embed_batch([question], insight_id)
        return vectors[0] if vectors else None

    def _format_hit(self, hit, columns_to_return: Optional[List[str]]) -> Dict[str, Any]:
        payload = dict(hit.payload or {})
        out: Dict[str, Any] = {
            "Score": float(hit.score) if hit.score is not None else 0.0,
            "id": hit.id,
        }
        if columns_to_return:
            for col in columns_to_return:
                if col in payload: out[col] = payload[col]
        else:
            out.update(payload)
        return out

    def _should_use_hybrid(self, use_hybrid_search_override: Optional[bool]) -> bool:
        if not self._is_hybrid_collection:
            return False
        if use_hybrid_search_override is None:
            return self.enable_hybrid_search
        return bool(use_hybrid_search_override)

    def _resolve_fusion(self):
        fusions = self._models.Fusion
        if self.fusion == "dbsf" and hasattr(fusions, "DBSF"):
            return fusions.DBSF
        return fusions.RRF

    def nearestNeighbor(
        self,
        question: str,
        limit: Optional[int] = 5,
        columns_to_return: Optional[List[str]] = None,
        score_threshold: Optional[Union[int, float]] = None,
        qdrant_filter: Optional[Dict[str, Any]] = None,
        insight_id: Optional[str] = None,
        use_hybrid_search: Optional[bool] = None,
        hybrid_prefetch_limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if not self._collection_ready:
            return []
        query_vector = self._embed_query(question, insight_id)
        if query_vector is None:
            return []

        effective_limit = int(limit) if limit is not None else 5
        flt = self._build_filter(qdrant_filter)
        hybrid = self._should_use_hybrid(use_hybrid_search)

        if hybrid:
            fetch_limit = int(hybrid_prefetch_limit) if hybrid_prefetch_limit is not None \
                else max(effective_limit * 4, 20)
            sparse_emb = self._encode_sparse_query(question)
            sparse_vec = self._to_sparse_vector(sparse_emb)
            dense_prefetch_kwargs: Dict[str, Any] = {
                "query": query_vector, "using": DENSE_VECTOR_NAME, "limit": fetch_limit,
            }
            sparse_prefetch_kwargs: Dict[str, Any] = {
                "query": sparse_vec, "using": SPARSE_VECTOR_NAME, "limit": fetch_limit,
            }
            if flt is not None:
                dense_prefetch_kwargs["filter"] = flt
                sparse_prefetch_kwargs["filter"] = flt
            fusion_enum = self._resolve_fusion()
            query_kwargs: Dict[str, Any] = {
                "collection_name": self.collection_name,
                "prefetch": [
                    self._models.Prefetch(**dense_prefetch_kwargs),
                    self._models.Prefetch(**sparse_prefetch_kwargs),
                ],
                "query": self._models.FusionQuery(fusion=fusion_enum),
                "limit": effective_limit,
                "with_payload": True,
                "with_vectors": False,
            }
        else:
            query_arg = query_vector
            using = DENSE_VECTOR_NAME if self._is_hybrid_collection else None
            query_kwargs = {
                "collection_name": self.collection_name,
                "query": query_arg,
                "limit": effective_limit,
                "with_payload": True,
                "with_vectors": False,
            }
            if using is not None:
                query_kwargs["using"] = using
            if flt is not None:
                query_kwargs["query_filter"] = flt

        if score_threshold is not None: query_kwargs["score_threshold"] = float(score_threshold)

        response = self.client.query_points(**query_kwargs)
        return [self._format_hit(h, columns_to_return) for h in response.points]

    def recommend(
        self,
        positive_ids: List[str],
        negative_ids: Optional[List[str]] = None,
        limit: Optional[int] = 5,
        columns_to_return: Optional[List[str]] = None,
        score_threshold: Optional[Union[int, float]] = None,
        qdrant_filter: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if not self._collection_ready or not positive_ids: return []
        flt = self._build_filter(qdrant_filter)
        recommend_input = self._models.RecommendInput(
            positive=list(positive_ids),
            negative=list(negative_ids) if negative_ids else [],
        )
        query_kwargs: Dict[str, Any] = {
            "collection_name": self.collection_name,
            "query": self._models.RecommendQuery(recommend=recommend_input),
            "limit": int(limit) if limit is not None else 5,
            "with_payload": True,
            "with_vectors": False,
        }
        if self._is_hybrid_collection:
            query_kwargs["using"] = DENSE_VECTOR_NAME
        if flt is not None: query_kwargs["query_filter"] = flt
        if score_threshold is not None: query_kwargs["score_threshold"] = float(score_threshold)
        response = self.client.query_points(**query_kwargs)
        return [self._format_hit(h, columns_to_return) for h in response.points]
