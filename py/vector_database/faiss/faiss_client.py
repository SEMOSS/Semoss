from typing import List, Dict, Union, Optional, Tuple
import atexit
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor

import pandas as pd
import faiss
import numpy as np
import os
import glob
import re

# CFG/SEMOSS packages
from gaas_gpt_model import ModelEngine
from ..constants import ENCODING_OPTIONS
from ..utils.bm25_client import BM25Searcher
from ._legacy_pickle_migration import migrate_pickle_to_safe

# ---------------------------------------------------------------------------
# Shared warmup pool
# ---------------------------------------------------------------------------
# Lazily-created process-wide thread pool used to load cold-start state
# (today: BM25 index + JSON corpus) in the background while __init__ returns.
# The on-disk loads are I/O bound, so two workers is enough to overlap the
# common case of one or two engines opening at once without saturating disk.
# Callers retrieve the value through a Future (`bm25_searcher` property), so
# the second-through-Nth caller naturally awaits the in-flight load rather
# than starting a duplicate.
_WARMUP_EXECUTOR: Optional[ThreadPoolExecutor] = None
_WARMUP_EXECUTOR_LOCK = threading.Lock()


def _warmup_executor() -> ThreadPoolExecutor:
    global _WARMUP_EXECUTOR
    if _WARMUP_EXECUTOR is None:
        with _WARMUP_EXECUTOR_LOCK:
            if _WARMUP_EXECUTOR is None:
                _WARMUP_EXECUTOR = ThreadPoolExecutor(
                    max_workers=2, thread_name_prefix="faiss-warmup"
                )
                atexit.register(
                    _WARMUP_EXECUTOR.shutdown, wait=False, cancel_futures=True
                )
    return _WARMUP_EXECUTOR


class FAISSSearcher:
    """
    The primary class for a faiss database classes and searching document embeddings
    """

    def __init__(
        self,
        embeddings_engine: ModelEngine,
        keywords_engine: ModelEngine,
        tokenizer,
        metric_type_is_cosine_similarity: bool,
        default_sort_direction: bool,
        base_path: str = None,
        reranker: str = "BAAI/bge-reranker-base",
        enable_hybrid_search: bool = True,
    ):
        self.class_logger = logging.getLogger(__name__)

        self._device_loaded = False
        self._device = None

        self.ds = None
        self.encoded_vectors = None
        self.vector_dimensions = None
        self.embeddings_engine = embeddings_engine
        self.keyword_engine = keywords_engine
        self.tokenizer = tokenizer
        self.metric_type_is_cosine_similarity = metric_type_is_cosine_similarity
        self.default_sort_direction = default_sort_direction
        self.base_path = base_path

        # One-shot conversion of any legacy .pkl files to safe formats (.parquet/.npy).
        migrate_pickle_to_safe(base_path)

        # Load existing files
        master_dataset_path = os.path.join(base_path, "dataset.parquet")
        master_vector_path = os.path.join(base_path, "vectors.npy")

        if not os.path.exists(master_dataset_path) or not os.path.exists(
            master_vector_path
        ):
            self.createMasterFiles(self.base_path)
        else:
            self.load_dataset(master_dataset_path)
            self.load_encoded_vectors(master_vector_path)

        # BM25 components are kicked off in a background warmup thread when hybrid
        # search is enabled: the JSON corpus + bm25s file load is I/O-heavy and
        # would otherwise dominate cold-start. Callers retrieve the searcher via
        # the `bm25_searcher` property, which awaits the same Future - so a
        # request that lands before the warmup finishes just blocks on it
        # instead of starting a duplicate load. If hybrid is disabled or the
        # background load fails we fall back to building inline on first access.
        self.enable_hybrid_search = enable_hybrid_search
        self._bm25_searcher = None
        self._bm25_lock = threading.Lock()
        self._bm25_future: Optional[Future] = None
        if self.enable_hybrid_search:
            self._bm25_future = _warmup_executor().submit(self._build_bm25_searcher)

        self.rerank = False
        self.reranker_model = None
        self.reranker_gaas_model = None
        self.reranker_tok = None
        self.reranker = reranker

    @property
    def ds(self):
        return self._ds

    @ds.setter
    def ds(self, value):
        if value is not None and not isinstance(value, pd.DataFrame):
            raise TypeError("ds must be a pandas.DataFrame")
        self._ds = value

    @property
    def encoded_vectors(self):
        return self._encoded_vectors

    @encoded_vectors.setter
    def encoded_vectors(self, value):
        if value is not None and not isinstance(value, np.ndarray):
            raise TypeError("encoded_vectors must be a np.ndarray")
        self._encoded_vectors = value

    @property
    def vector_dimensions(self):
        return self._vector_dimensions

    @vector_dimensions.setter
    def vector_dimensions(self, value):
        if value is not None and not isinstance(value, tuple):
            raise TypeError("vector_dimensions must be a tuple")
        self._vector_dimensions = value

    @property
    def base_path(self):
        return self._base_path

    @base_path.setter
    def base_path(self, value):
        if value is not None and not isinstance(value, str):
            raise TypeError("base_path must be a string")
        self._base_path = value

    @property
    def bm25_searcher(self):
        """Return the BM25 searcher, awaiting the background warmup if needed.

        Returns None when hybrid search is disabled. Otherwise:
          - If `__init__` submitted a warmup Future and it's still running,
            this blocks until the Future completes; concurrent callers all
            await the same Future (no duplicate loads).
          - If the warmup Future failed, falls back to an inline build under a
            lock so subsequent reads don't keep re-raising the original error.
          - If `__init__` never submitted a warmup (e.g. the field was reset),
            builds inline under the lock.
        After the first successful load the cached searcher is returned with
        no synchronization overhead.
        """
        if not self.enable_hybrid_search:
            return None
        if self._bm25_searcher is not None:
            return self._bm25_searcher
        with self._bm25_lock:
            if self._bm25_searcher is not None:
                return self._bm25_searcher
            future = self._bm25_future
            if future is not None:
                try:
                    self._bm25_searcher = future.result()
                except Exception:
                    self.class_logger.exception(
                        "Background BM25 warmup failed; building inline"
                    )
                finally:
                    self._bm25_future = None
            if self._bm25_searcher is None:
                self._bm25_searcher = self._build_bm25_searcher()
            return self._bm25_searcher

    def _build_bm25_searcher(self) -> BM25Searcher:
        """Construct and load the BM25 searcher. Called either by the background
        warmup or inline from the `bm25_searcher` property as a fallback."""
        searcher = BM25Searcher(base_path=self.base_path)
        searcher.generate_and_load_bm25_index(self.ds)
        return searcher

    # ensure that device is lazy loaded to avoid heavy torch import as long as possible
    def __getattr__(self, name):
        """
        Called only if 'name' is not found in the normal attribute dictionary.
        We use it to lazy-load a specific attribute.
        """
        if name == "device":
            if not self._device_loaded:
                self._init_device()
            return self._device
        # If it's not the special attribute, raise AttributeError
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

    def _init_device(self):
        """
        Utility method to determine whether or not the device running the interpreter has a gpu
        """
        self.class_logger.info(f"Loading torch in faiss client")
        import torch

        self._device = (
            torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
        )
        self._device_loaded = True
        self.class_logger.info(f"Done loading torch in faiss client")

    def nearestNeighbor(
        self,
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
        Enhanced nearest neighbor search with optional hybrid BM25 + vector search
        """
        use_hybrid = (
            use_hybrid_search
            if use_hybrid_search is not None
            else self.enable_hybrid_search
        )

        if not use_hybrid or self.bm25_searcher is None:
            # if im not hybrid im using original vector-only search
            return self._vector_only_search(
                question=question,
                filter=filter,
                limit=limit,
                columns_to_return=columns_to_return,
                return_threshold=return_threshold,
                total_limit=total_limit,
                insight_id=insight_id,
            )

        return self._hybrid_search(
            question=question,
            filter=filter,
            limit=limit,
            columns_to_return=columns_to_return,
            return_threshold=return_threshold,
            total_limit=total_limit,
            vector_weight=vector_weight,
            bm25_weight=bm25_weight,
            insight_id=insight_id,
        )

    def _hybrid_search(
        self,
        question: str,
        filter: str,
        limit: int,
        columns_to_return: Optional[List[str]],
        return_threshold: Optional[Union[int, float]],
        total_limit: int,
        vector_weight: Optional[Union[int, float]],
        bm25_weight: Optional[Union[int, float]],
        insight_id: str,
    ):
        """Perform hybrid BM25 + vector search"""
        if columns_to_return is None:
            columns_to_return = list(self.ds.columns)

        fusion_limit = max(total_limit * 2, limit * 2, 20)

        # 1. Do vector search
        vector_results = self._vector_only_search(
            question=question,
            filter=filter,
            limit=fusion_limit,
            columns_to_return=columns_to_return,
            return_threshold=return_threshold,
            total_limit=fusion_limit,
            insight_id=insight_id,
        )

        # 2. Do BM25 search
        # BM25 index was built on full dataset, so we must use full dataset for lookup
        bm25_results = self.bm25_searcher.search_with_data(
            question,
            top_k=fusion_limit,
            columns_to_return=columns_to_return,
            ds=self.ds,
        )

        # Filter BM25 results to only include documents that match the filter
        if filter is not None:
            filter_ids = self._filter_dataset(filter)
            filter_ids_set = set(filter_ids)
            bm25_results = [
                result for result in bm25_results if result["idx"] in filter_ids_set
            ]

        # 3. Combine using reciprocal rank fusion
        if bm25_results:
            # if no user defined weights, try to predict
            if (
                vector_weight is None
                or vector_weight <= 0
                or bm25_weight is None
                or bm25_weight <= 0
            ):
                pred_weights = self.estimate_weights(question)
            else:
                pred_weights = (vector_weight, bm25_weight)

            hybrid_results = self._weighted_rank_fusion(
                vector_results, bm25_results, *pred_weights
            )
        else:
            # fall back to vector-only if BM25 failed
            hybrid_results = vector_results

        # 4. return top results
        return hybrid_results[:limit]

    def _vector_only_search(
        self,
        question: str,
        filter: Optional[str],
        limit: int,
        columns_to_return: Optional[List[str]],
        return_threshold: float,
        total_limit: int,
        insight_id: Optional[str],
    ) -> List[Dict]:
        """Original vector-only search logic"""
        if columns_to_return is None:
            columns_to_return = list(self.ds.columns)

        search_vector = self.embeddings_engine.embeddings(
            strings_to_embed=[question], insight_id=insight_id
        )

        if isinstance(search_vector, List):
            query_vector = np.array(search_vector[0]["response"], dtype=np.float32)
        else:
            query_vector = np.array(search_vector["response"], dtype=np.float32)
        assert query_vector.shape[0] == 1

        if type(self.tokenizer).__name__ == "HuggingfaceTokenizer":
            faiss.normalize_L2(query_vector)

        if not isinstance(limit, int):
            limit = int(limit)

        if not self.rerank:
            total_limit = limit

        if filter != None:
            filter_ids = self._filter_dataset(filter)
            id_selector = faiss.IDSelectorArray(filter_ids)
            distances, ann_index = self.index.search(
                query_vector,
                k=total_limit,
                params=faiss.SearchParametersIVF(sel=id_selector),
            )
        else:
            distances, ann_index = self.index.search(query_vector, k=total_limit)

        distances = distances[0]
        ann_index = ann_index[0]

        if self.rerank:
            return self.do_rerank(
                question=question,
                distances=distances,
                ann_index=ann_index,
                result_count=limit,
                columns_to_return=columns_to_return,
            )

        if self.vector_dimensions[0] < limit:
            index_of_minus_one = np.where(ann_index == -1)[0]
            if len(index_of_minus_one) > 0:
                ann_index = ann_index[: index_of_minus_one[0]]
                distances = distances[: index_of_minus_one[0]]

        samples_df = pd.DataFrame({"distances": distances, "ann": ann_index})
        samples_df.sort_values(
            "distances",
            ascending=self.default_sort_direction,
            inplace=True,
        )
        samples_df = samples_df[samples_df["distances"] <= return_threshold]

        final_output = []
        for _, row in samples_df.iterrows():
            output = {"Score": row["distances"], "idx": int(row["ann"])}
            data_row = self.ds.iloc[int(row["ann"])]
            output.update({col: data_row[col] for col in columns_to_return})
            final_output.append(output)

        return final_output

    def _reciprocal_rank_fusion(
        self,
        vector_results: List[Dict],
        bm25_results: List[Dict],
        k: int = 60,
    ) -> List[Dict]:
        """
        Combine vector and BM25 results using Reciprocal Rank Fusion

        Args:
            vector_results: Results from vector search with 'Score' and data and index
            bm25_results: Results from BM25 search with 'BM25_SCORE' and data and index
            k: RRF parameter (typically 60)
        """
        # this will store doc_index to RRF_Score and search result dict
        combined_scores = {}
        # go through the vector results - these will all be new additions
        for i, result in enumerate(vector_results):
            doc_idx = result["idx"]
            copy_result = result.copy()
            copy_result.update({"RRF_Score": 1.0 / (k + i + 1)})
            combined_scores[doc_idx] = {
                "RRF_Score": copy_result["RRF_Score"],
                "result": copy_result,
            }

        # go through the bm25 results
        # merge the RRF_Score if the document also showed up in vector search
        for i, result in enumerate(bm25_results):
            doc_idx = result["idx"]
            copy_result = result.copy()
            copy_result.update({"Score": -1})
            copy_result.update({"RRF_Score": 1.0 / (k + i + 1)})

            if doc_idx in combined_scores:
                combined_scores[doc_idx]["RRF_Score"] += copy_result["RRF_Score"]
                # add the BM25_Score into the result map
                combined_scores[doc_idx]["result"]["BM25_Score"] = copy_result[
                    "BM25_Score"
                ]
            else:
                combined_scores[doc_idx] = {
                    "RRF_Score": copy_result["RRF_Score"],
                    "result": copy_result,
                }

        sorted_results = sorted(
            combined_scores.items(), key=lambda x: x[1]["RRF_Score"], reverse=True
        )

        final_results = [item[1]["result"] for item in sorted_results]
        return final_results

    def _weighted_rank_fusion(
        self,
        vector_results: List[Dict],
        bm25_results: List[Dict],
        vector_weight: float = 0.5,
        bm25_weight: float = 0.5,
        k: int = 60,
    ) -> List[Dict]:
        """
        Combine vector and BM25 results using Weighted Rank Fusion

        Args:
            vector_results: Results from vector search with 'Score' and data and index
            bm25_results: Results from BM25 search with 'BM25_SCORE' and data and index
            vector_weight: Weight for vector search results (default 0.5)
            bm25_weight: Weight for BM25 search results (default 0.5)
            k: RRF parameter (typically 60)

        Note:
            Weights don't need to sum to 1.0, but larger weights give more importance
            to that search method. For example:
            - vector_weight=0.7, bm25_weight=0.3: Favor semantic search
            - vector_weight=0.3, bm25_weight=0.7: Favor keyword search
            - vector_weight=1.0, bm25_weight=1.0: Equal weighting (similar to original RRF)
        """
        # Normalize weights to sum to 1.0 for consistent scoring
        total_weight = vector_weight + bm25_weight
        norm_vector_weight = vector_weight / total_weight
        norm_bm25_weight = bm25_weight / total_weight

        # Store doc_index to weighted RRF score and search result dict
        combined_scores = {}

        # Process vector results with vector weight
        for i, result in enumerate(vector_results):
            doc_idx = result["idx"]
            copy_result = result.copy()
            weighted_score = norm_vector_weight * (1.0 / (k + i + 1))
            copy_result.update({"Weighted_RRF_Score": weighted_score})
            combined_scores[doc_idx] = {
                "Weighted_RRF_Score": weighted_score,
                "result": copy_result,
            }

        # Process BM25 results with BM25 weight
        for i, result in enumerate(bm25_results):
            doc_idx = result["idx"]
            copy_result = result.copy()
            copy_result.update({"Score": -1})
            weighted_score = norm_bm25_weight * (1.0 / (k + i + 1))
            copy_result.update({"Weighted_RRF_Score": weighted_score})

            if doc_idx in combined_scores:
                # Add the weighted BM25 score to existing vector score
                combined_scores[doc_idx]["Weighted_RRF_Score"] += weighted_score
                # Add the BM25_Score into the result map
                combined_scores[doc_idx]["result"]["BM25_Score"] = copy_result[
                    "BM25_Score"
                ]
            else:
                combined_scores[doc_idx] = {
                    "Weighted_RRF_Score": weighted_score,
                    "result": copy_result,
                }

        # Sort by weighted RRF score
        sorted_results = sorted(
            combined_scores.items(),
            key=lambda x: x[1]["Weighted_RRF_Score"],
            reverse=True,
        )

        final_results = [item[1]["result"] for item in sorted_results]
        return final_results

    def estimate_weights(self, query: str) -> tuple[float, float]:
        """
        Very basic logic to determine if query should favor a vector search of keyword search

        Args:
            query: The question being asked

        Returns:
            Tuple containing the (vector_weight, bm25_weight)

        Note:
            - Only accounts for English language.

        TODO: expose different methods including LLM to determine weights
        """
        query_lower = query.lower()
        words = query.split()

        bm25_score = 0
        vector_score = 0

        # BM25 indicators
        if len(words) <= 3:
            bm25_score += 2
        if any(char in query for char in ['"', "#", "-", "_"]):
            bm25_score += 3  # special chars suggest exact matching
        if any(word.isupper() for word in words):
            bm25_score += 2  # acronyms
        if re.search(r"\d+", query):
            bm25_score += 1  # contains numbers
        if not any(
            q in query_lower for q in ["how", "what", "why", "when", "where", "who"]
        ):
            bm25_score += 1  # not a question

        # Vector indicators
        if len(words) >= 7:
            vector_score += 2
        if any(
            q in query_lower
            for q in ["how to", "best way", "explain", "understand", "concept"]
        ):
            vector_score += 3
        if query.endswith("?"):
            vector_score += 2
        if any(word in query_lower for word in ["similar", "like", "related", "about"]):
            vector_score += 2

        # Convert to weights (default to balanced if unclear)
        if bm25_score > vector_score + 2:
            return (0.3, 0.7)
        elif vector_score > bm25_score + 2:
            return (0.7, 0.3)
        else:
            return (0.5, 0.5)

    def list_documents(self) -> List[str]:
        """
        Get the unique list of documents
        """
        if self.ds is not None:
            return self.ds["Source"].unique().tolist()

        return []

    def list_all_records(self) -> List[dict]:
        """
        Get the list of all the records
        """
        if self.ds is not None:
            return self.ds.sort_values(by=["Source", "Divider", "Part"]).to_dict(
                "records"
            )

        return []

    def _filter_dataset(self, filter: str) -> List[int]:
        return self.ds.query(filter).index.to_list()

    def load_dataset(self, dataset_location: str) -> None:
        """
        Utility method to load stored datasets into the object.

        Args:
        dataset_location(`str`):
            The file path to the stored dataset. Supported file types are csv and parquet.
            Legacy `.pkl` paths are transparently rewritten to `.parquet` for callers that
            still reference the old extension.

        Returns:
        `None`
        """
        if dataset_location.endswith(".pkl"):
            self.class_logger.warning(
                "load_dataset called with legacy .pkl path; reading the migrated .parquet file instead: %s",
                dataset_location,
            )
            dataset_location = dataset_location[: -len(".pkl")] + ".parquet"
        self.ds = self._load_dataset(dataset_location=dataset_location)

    def _load_dataset(self, dataset_location: str) -> pd.DataFrame:
        """
        Internal method to load the dataset based on its file type.

        Args:
        dataset_location(`str`):
            The file path to the stored dataset. Supported file types are csv and parquet.

        Returns:
        `pd.DataFrame`
        """
        loaded_dataset = None
        if dataset_location.endswith(".csv"):
            for encoding in tuple(ENCODING_OPTIONS):
                try:
                    loaded_dataset = pd.read_csv(dataset_location, encoding=encoding)
                    break
                except Exception:
                    continue
            if loaded_dataset is None:
                raise Exception(
                    "Unable to read the file with any of the specified encodings"
                )
        elif dataset_location.endswith(".parquet"):
            loaded_dataset = pd.read_parquet(dataset_location)
        else:
            raise ValueError(
                "Dataset creation for provided file type has not been defined"
            )

        return loaded_dataset

    def save_dataset(self, dataset_location: str) -> None:
        """
        Utility method to save datasets from object onto the disk as Parquet.

        Args:
        dataset_location(`str`):
            The file path to the write the dataset. Must end in `.parquet`.

        Returns:
        `None`
        """
        if not isinstance(self.ds, pd.DataFrame):
            raise TypeError(
                f"save_dataset requires pandas.DataFrame, got {type(self.ds).__name__}"
            )
        # Normalize columns to canonical dtypes before writing. Per-source
        # parquet files can disagree on Divider/Part dtype (legacy HF-Dataset
        # writes used string; pandas inference from CSV can yield int), and the
        # concatenation produces an object column with mixed Python types that
        # pyarrow refuses to serialize.
        self._enforce_canonical_schema(self.ds)
        self.ds.to_parquet(dataset_location, index=False)

    @staticmethod
    def _enforce_canonical_schema(df: pd.DataFrame) -> None:
        """Cast the standard CFG columns to their canonical dtypes in place.

        Cheap when columns already have the expected dtype, so it can be called
        at every write site without paying a noticeable cost in the common case.
        """
        for col in ("Source", "Divider", "Part", "Content"):
            if col in df.columns:
                df[col] = df[col].astype(str)
        if "Tokens" in df.columns:
            df["Tokens"] = df["Tokens"].astype("int64")

    def load_encoded_vectors(self, encoded_vectors_location: str) -> None:
        """
        Utility method to load stored embeddings from the disk.

        Args:
        encoded_vectors_location(`str`):
            The file path to the stored embeddings file. Supported file type is `.npy`.
            Legacy `.pkl` paths are transparently rewritten to `.npy` for callers that
            still reference the old extension.

        Returns:
        `None`
        """
        if encoded_vectors_location.endswith(".pkl"):
            self.class_logger.warning(
                "load_encoded_vectors called with legacy .pkl path; reading the migrated .npy file instead: %s",
                encoded_vectors_location,
            )
            encoded_vectors_location = encoded_vectors_location[: -len(".pkl")] + ".npy"
        self.encoded_vectors = self._load_encoded_vectors(
            encoded_vectors_location=encoded_vectors_location
        )
        self.vector_dimensions = self.encoded_vectors.shape

        if self.metric_type_is_cosine_similarity:
            self.index = faiss.index_factory(
                self.vector_dimensions[1], "Flat", faiss.METRIC_INNER_PRODUCT
            )
        else:
            self.index = faiss.IndexFlatL2(self.vector_dimensions[1])

        self.index.add(self.encoded_vectors)

    def _load_encoded_vectors(self, encoded_vectors_location: str) -> np.ndarray:
        """
        Internal method to load stored embeddings from the disk

        Args:
        encoded_vectors_location(`str`):
            The file path to the stored embeddings file. Only `.npy` files are supported.

        Returns:
        `None`
        """
        if not encoded_vectors_location.endswith(".npy"):
            raise ValueError(
                f"Encoded vectors must be loaded from a .npy file, got: {encoded_vectors_location}"
            )
        # allow_pickle=False ensures np.load cannot deserialize arbitrary Python objects.
        encoded_vectors = np.load(
            encoded_vectors_location, allow_pickle=False, mmap_mode="r"
        )

        assert isinstance(encoded_vectors, np.ndarray)

        return encoded_vectors

    def save_encoded_vectors(self, encoded_vectors_location: str) -> None:
        """
        Utility method to save embeddings from object onto the disk as a `.npy` file.

        Args:
        encoded_vectors_location(`str`):
            The file path to the write the dataset. Must end in `.npy`.

        Returns:
        `None`
        """
        np.save(encoded_vectors_location, self.encoded_vectors, allow_pickle=False)

    def _concatenate_datasets(
        self,
        datasets: List[pd.DataFrame],
    ) -> pd.DataFrame:
        """
        Internal utility method to concatenate a list of pandas DataFrames.

        Args:
        datasets(`List[pd.DataFrame]`):
            A list of DataFrames to concatenate.

        Returns:
        `pd.DataFrame`
        """
        return pd.concat(datasets, ignore_index=True)

    def addDocument(
        self,
        documentFileLocation: List[str],
        columns_to_index: Optional[List[str]],
        columns_to_remove: Optional[List[str]] = [],
        target_column: Optional[str] = "text",
        separator: Optional[str] = ",",
        keyword_search_params: Optional[Dict] = {},
        insight_id: Optional[str] = None,
    ) -> Dict:
        """
        Enhanced document addition with BM25 index updates
        """
        # Call the original addDocument method
        response = self._vector_addDocument(
            documentFileLocation,
            columns_to_index,
            columns_to_remove,
            target_column,
            separator,
            keyword_search_params,
            insight_id,
        )

        if self.enable_hybrid_search and self.ds is not None:
            try:
                # Extract all text content for BM25 indexing
                if "Content" in self.ds.columns:
                    all_texts = list(self.ds["Content"])
                else:
                    # Fallback: concatenate all text fields
                    all_texts = []
                    for i in range(len(self.ds)):
                        row = self.ds.iloc[i]
                        text_parts = [
                            value for value in row.values if isinstance(value, str)
                        ]
                        all_texts.append(" ".join(text_parts))

                if self.bm25_searcher is not None:
                    self.bm25_searcher.build_bm25_index(all_texts)

            except Exception as e:
                self.class_logger.error(f"Failed to update BM25 index: {e}")

        return response

    def _vector_addDocument(
        self,
        documentFileLocation: List[str],
        columns_to_index: Optional[List[str]],
        columns_to_remove: Optional[List[str]] = [],
        target_column: Optional[str] = "text",
        separator: Optional[str] = ",",
        keyword_search_params: Optional[Dict] = {},
        insight_id: Optional[str] = None,
    ) -> Dict:
        """
        Given a path to a CSV document, perform the following tasks:
        - concatenate the columns the embeddings should be created from
        - get the embeddings for all the extracted chunks in the document
        - `Optional` - remove the columns that are not supposed to be stored based on columns_to_remove param
        - write out both the dataset and embeddings objects onto the disk so they can be reloaded or removed

        Args:
        documentFileLocation(`List[str]`):
            A list of document file location to create embeddings from
        columns_to_index(`List[str]`):
            A list of column names to create the index from. These columns will be concatenated.
        columns_to_remove(`List[str]`):
            A list of column names that should not be stored in the dataset. This will never be returned in nearestNeighbor search because they will no longer exist.
        target_column(`str`):
            The column name for the concatenated columns from which the embeddings will be created
        separator(`str`):
            The character to use as a delimeter between columns for the concatenated column that the embeddings will be created from
        keyword_search_params (`Dict`):
            A dictionary containing the keyword search parameters
        insight_id(`str`):
            The unique identifier of the insight from which the call is being made

        Returns:
            `Dict` with `createdDocuments` (artifact paths written) and `documentStatuses`
            (one SUCCESS/PARTIAL/FAILED entry per input CSV, in input order, with
            insertedRecords/failedRecords/totalRecords and an error message on failure).
            Embedding is best-effort: a document that fails is reported FAILED and rolled
            back while the remaining documents are still processed.
        """
        # make sure they are all in indexed_files dir
        assert {
            os.path.basename(os.path.dirname(path)) for path in documentFileLocation
        } == {"indexed_files"}

        # create a list of the documents created so that we can push the files back to the cloud
        # documentStatuses reports one SUCCESS/PARTIAL/FAILED entry per input CSV so a
        # single bad document no longer fails the whole batch (best-effort embedding)
        createDocumentsResponse = {
            "createdDocuments": [],
            "documentStatuses": [],
        }
        # per-document [(artifact_paths, rows)] aligned with documentStatuses
        per_document_artifacts = []

        # Embed each input CSV in one call, then preserve the existing per-Source
        # dataset/vector artifacts used by removal and master-index rebuilding.
        for document in documentFileLocation:
            file_name = os.path.basename(document)
            # _append_vectors always rebinds to new arrays, so holding the previous
            # references is a complete rollback for a failed document
            snapshot_vectors = self.encoded_vectors
            snapshot_dimensions = self.vector_dimensions
            created_artifacts = []
            total_rows = 0
            try:
                dataset = self._load_dataset(dataset_location=document)
                total_rows = len(dataset)
                sources = dataset["Source"].unique().tolist()
                directory = os.path.dirname(document)
                effective_columns = (
                    list(dataset.columns)
                    if columns_to_index is None or len(columns_to_index) == 0
                    else columns_to_index
                )
                keyword_params = dict(keyword_search_params or {})
                keyword_search = keyword_params.pop("keywordSearch", None) is True
                prepared_sources = []

                for source_name in sources:
                    source_dataset = dataset[
                        dataset["Source"] == source_name
                    ].reset_index(drop=True)
                    if len(source_dataset) == 0:
                        continue
                    parts = source_dataset[effective_columns].astype(str)
                    source_dataset[target_column] = parts.apply(
                        lambda row: separator.join(row) + separator, axis=1
                    )
                    if keyword_search:
                        source_dataset[target_column] = (
                            self.keyword_engine.keyword_extraction(
                                input=list(source_dataset[target_column]),
                                insight_id=insight_id,
                                param_dict=keyword_params,
                            )
                        )
                        vectors = self._embed_and_validate(
                            list(source_dataset[target_column]), insight_id
                        )
                        created_artifacts.append(
                            (
                                self._persist_source_artifacts(
                                    directory,
                                    source_name,
                                    source_dataset,
                                    vectors,
                                    columns_to_remove,
                                    target_column,
                                ),
                                len(source_dataset),
                            )
                        )
                        self._append_vectors(vectors)
                    else:
                        prepared_sources.append((source_name, source_dataset))

                if prepared_sources:
                    all_text = [
                        value
                        for _source_name, source_dataset in prepared_sources
                        for value in source_dataset[target_column].tolist()
                    ]
                    all_vectors = self._embed_and_validate(all_text, insight_id)
                    offset = 0
                    for source_name, source_dataset in prepared_sources:
                        source_rows = len(source_dataset)
                        source_vectors = all_vectors[offset : offset + source_rows]
                        offset += source_rows
                        created_artifacts.append(
                            (
                                self._persist_source_artifacts(
                                    directory,
                                    source_name,
                                    source_dataset,
                                    source_vectors,
                                    columns_to_remove,
                                    target_column,
                                ),
                                source_rows,
                            )
                        )
                    if offset != len(all_vectors):
                        raise ValueError(
                            "Embedding rows could not be assigned to their Sources"
                        )
                    self._append_vectors(all_vectors)
            except Exception as error:
                self.class_logger.exception(
                    "Embedding failed for document %s", document
                )
                for artifact_paths, _rows in created_artifacts:
                    for artifact_path in artifact_paths:
                        try:
                            os.remove(artifact_path)
                        except OSError:
                            pass
                self.encoded_vectors = snapshot_vectors
                self.vector_dimensions = snapshot_dimensions
                per_document_artifacts.append([])
                createDocumentsResponse["documentStatuses"].append(
                    {
                        "fileName": file_name,
                        "status": "FAILED",
                        "insertedRecords": 0,
                        "failedRecords": total_rows,
                        "totalRecords": total_rows,
                        "error": str(error),
                    }
                )
                continue

            inserted_rows = sum(rows for _paths, rows in created_artifacts)
            for artifact_paths, _rows in created_artifacts:
                createDocumentsResponse["createdDocuments"].extend(artifact_paths)
            per_document_artifacts.append(created_artifacts)
            createDocumentsResponse["documentStatuses"].append(
                {
                    "fileName": file_name,
                    "status": "SUCCESS",
                    "insertedRecords": inserted_rows,
                    "failedRecords": total_rows - inserted_rows,
                    "totalRecords": total_rows,
                }
            )

        master_indexClass_files, corrupted_file_sets = self.createMasterFiles(
            path_to_files=os.path.dirname(os.path.dirname(documentFileLocation[0]))
        )

        for corrupted_set in corrupted_file_sets:
            for file_path in corrupted_set:
                createDocumentsResponse["createdDocuments"].remove(file_path)

        # downgrade documents whose artifacts failed post-write validation so the
        # removal is reported instead of silently shrinking createdDocuments
        corrupted_paths = {
            file_path
            for corrupted_set in corrupted_file_sets
            for file_path in corrupted_set
        }
        if corrupted_paths:
            for status, created_artifacts in zip(
                createDocumentsResponse["documentStatuses"], per_document_artifacts
            ):
                lost_rows = sum(
                    rows
                    for artifact_paths, rows in created_artifacts
                    if corrupted_paths.intersection(artifact_paths)
                )
                if status["status"] != "SUCCESS" or lost_rows == 0:
                    continue
                status["insertedRecords"] -= lost_rows
                status["failedRecords"] += lost_rows
                status["status"] = (
                    "FAILED" if status["insertedRecords"] == 0 else "PARTIAL"
                )
                status["error"] = (
                    "Source artifacts failed validation after writing and were removed"
                )

        createDocumentsResponse["createdDocuments"].extend(master_indexClass_files)

        return createDocumentsResponse

    def _embed_and_validate(
        self, strings_to_embed: List[str], insight_id: Optional[str]
    ) -> np.ndarray:
        """Embed one bounded CSV batch and validate it before writing artifacts."""
        response = self.embeddings_engine.embeddings(
            strings_to_embed=strings_to_embed,
            insight_id=insight_id,
        )
        try:
            vectors = np.asarray(response[0]["response"], dtype=np.float32)
        except (IndexError, KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "Embedding response does not contain a numeric response matrix"
            ) from error
        if (
            vectors.ndim != 2
            or vectors.shape[0] != len(strings_to_embed)
            or vectors.shape[1] == 0
        ):
            raise ValueError(
                "Embedding response shape does not match the submitted rows: "
                f"expected {len(strings_to_embed)} rows, received {vectors.shape}"
            )
        if (
            self.vector_dimensions is not None
            and self.vector_dimensions[1] != vectors.shape[1]
        ):
            raise ValueError(
                "Embedding dimensions do not match the existing FAISS index: "
                f"expected {self.vector_dimensions[1]}, received {vectors.shape[1]}"
            )
        if type(self.tokenizer).__name__ == "HuggingfaceTokenizer":
            faiss.normalize_L2(vectors)
        return vectors

    def _persist_source_artifacts(
        self,
        directory: str,
        source_name: str,
        source_dataset: pd.DataFrame,
        vectors: np.ndarray,
        columns_to_remove: Optional[List[str]],
        target_column: str,
    ) -> List[str]:
        """Write the stable dataset and vector pair for one Source."""
        if len(source_dataset) != len(vectors):
            raise ValueError(
                f"Source {source_name} has {len(source_dataset)} rows but {len(vectors)} vectors"
            )
        columns_to_drop = list(
            set([*(columns_to_remove or []), target_column]).intersection(
                set(source_dataset.columns)
            )
        )
        stored_dataset = source_dataset.drop(columns=columns_to_drop)
        self._enforce_canonical_schema(stored_dataset)
        dataset_path = os.path.join(directory, source_name + "_dataset.parquet")
        vector_path = os.path.join(directory, source_name + "_vectors.npy")
        stored_dataset.to_parquet(dataset_path, index=False)
        np.save(vector_path, vectors, allow_pickle=False)
        return [dataset_path, vector_path]

    def _append_vectors(self, vectors: np.ndarray) -> None:
        """Maintain the in-memory vector shape until master files are rebuilt."""
        if self.encoded_vectors is None:
            self.encoded_vectors = np.copy(vectors)
            self.vector_dimensions = self.encoded_vectors.shape
            return
        if self.vector_dimensions[1] != vectors.shape[1]:
            raise ValueError(
                "Embedding dimensions do not match the existing FAISS index: "
                f"expected {self.vector_dimensions[1]}, received {vectors.shape[1]}"
            )
        self.encoded_vectors = np.concatenate([self.encoded_vectors, vectors], axis=0)
        self.vector_dimensions = self.encoded_vectors.shape

    def createMasterFiles(self, path_to_files: str) -> Tuple[str]:
        """
        Create a master dataset and embeddings file based on the current documents. The main purpose of this is to improve startup runtime.

        Args:
        path_to_files(`str`):
            The folder location of the indexed documents/datasets/embeddings

        Returns:
        `List[str]`
        """
        created_documents, corrupted_file_sets = self._validateEmbeddingFiles(
            path_to_files=path_to_files,
        )

        return created_documents, corrupted_file_sets

    def _validateEmbeddingFiles(self, path_to_files: str, delete: bool = True) -> Tuple:
        """
        This method aims to validate the existing dataset and vector files and create new ones if necessary. It takes the path to the files and a boolean to determine if corrupted files should be deleted.

        The function operates by locating and loading all files within a specified directory and ensures there is a corresponding dataset and vector file.

        In case dataset or vector files are not found, it records the files under corrupted sets.

        Only documents with valid and verified dataset and vector files are stored for analysis or further usage.

        The function will additionally delete all identified corrupted files if delete is set to True. The final result is the list of created file paths, corrupted documents and the file data sets that were identified as corrupted.

        Args:
        path_to_files(`str`):
            The folder location of the index class/collection e.g. schema/default

        Returns `Tuple`: A tuple containing created_documents, corrupted_file_sets
            - created_documents is a `List[str]` containing the names master files names for files created during the validation process
            - corrupted_file_sets is a `List[Tuple]` with the csv, dataset, vector and source files paths for corrupted sets
        """
        indexed_files_path = os.path.join(path_to_files, "indexed_files")

        # If any legacy .pkl files remain in this subtree, convert them now so the
        # globs below find their migrated counterparts.
        migrate_pickle_to_safe(path_to_files)

        # List all vector files in the directory
        all_vector_files = glob.glob(os.path.join(indexed_files_path, "*_vectors.npy"))

        valid_datasets_and_vectors = []
        corrupted_file_sets: List[Tuple] = []
        created_documents: List[str] = []

        for full_vector_path in all_vector_files:
            # get the basename of the file
            # all csvs, datasets and vectors should contain this base name
            vector_filename = os.path.basename(full_vector_path)
            base_filename = vector_filename.split("_vectors.npy")[0]
            dataset_filename = base_filename + "_dataset.parquet"

            full_dataset_path = os.path.join(indexed_files_path, dataset_filename)
            full_vector_path = os.path.join(indexed_files_path, vector_filename)

            # if all the file paths exist, then create the tuple
            if os.path.exists(full_dataset_path) and os.path.exists(full_vector_path):
                # the next step is to validate non of these files are corrupted by attempting to load them all in

                try:
                    # try load the dataset
                    dataset = self._load_dataset(dataset_location=full_dataset_path)
                except Exception as e:
                    corrupted_file_sets.append(
                        (
                            full_dataset_path,
                            full_vector_path,
                        )
                    )
                    continue

                try:
                    # try load the vectors
                    vectors = self._load_encoded_vectors(
                        encoded_vectors_location=full_vector_path
                    )
                except:
                    corrupted_file_sets.append(
                        (
                            full_dataset_path,
                            full_vector_path,
                        )
                    )
                    continue

                # if we made it this far then all the files are not corrupted
                valid_datasets_and_vectors.append((dataset, vectors))

        # bind the valid datasets and vectors
        if len(valid_datasets_and_vectors) > 0:
            dfs = [d for d, _ in valid_datasets_and_vectors]
            vecs = [v for _, v in valid_datasets_and_vectors]

            self.ds = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
            self.encoded_vectors = (
                np.concatenate(vecs, axis=0) if len(vecs) > 1 else vecs[0]
            )
            self.vector_dimensions = self.encoded_vectors.shape

            encoded_vectors_location = os.path.join(path_to_files, "vectors.npy")
            dataset_location = os.path.join(path_to_files, "dataset.parquet")
            self.save_encoded_vectors(encoded_vectors_location=encoded_vectors_location)
            self.save_dataset(dataset_location=dataset_location)
            created_documents.append(encoded_vectors_location)
            created_documents.append(dataset_location)

            if (
                self.metric_type_is_cosine_similarity
                and self.vector_dimensions is not None
            ):
                self.index = faiss.index_factory(
                    self.vector_dimensions[1], "Flat", faiss.METRIC_INNER_PRODUCT
                )
            elif self.vector_dimensions is not None:
                self.index = faiss.IndexFlatL2(self.vector_dimensions[1])

            if self.encoded_vectors is not None:
                self.index.add(self.encoded_vectors)

        if delete:
            for corrupted in corrupted_file_sets:
                for filename in corrupted:
                    try:
                        os.remove(filename)
                    except FileNotFoundError:
                        pass

        return created_documents, corrupted_file_sets

    def removeCorruptedFiles(self, path_to_files: str) -> List[Tuple]:
        """
        Check the vector index class/ collection for corrupted files and recreate the master files.

        Args:
        path_to_files(`str`):
            The folder location of the index class/collection e.g. schema/default

        Returns `List[Tuple]`: A list of tuples containing the csv, dataset, vector and source files paths for corrupted sets
        """
        corrupted_files = self._validateEmbeddingFiles(
            path_to_files=path_to_files,
        )[1]

        return corrupted_files

    def datasetsLoaded(self) -> bool:
        """
        Check if data was loaded in from the csv

        Returns `bool`
        """
        if self.ds is None or len(self.ds.columns) == 0 or len(self.ds) == 0:
            return False
        else:
            return True

    def do_rerank(
        self,
        question: str,
        distances: List,
        ann_index: List[int],
        result_count: int,
        columns_to_return: Optional[List[str]] = None,
    ):
        # reranks based on an algorithm and then finds
        if self.reranker_gaas_model is None:
            self.init_reranker()

        samples_df = pd.DataFrame({"distances": distances, "ann": ann_index})

        final_output = []
        reranker_call_success = True
        for _, row in samples_df.iterrows():
            output = {}
            output.update({"Score": row["distances"]})
            data_row = self.ds.iloc[int(row["ann"])]
            output.update({col: data_row[col] for col in columns_to_return})

            # this is not pythonic but let us try this for now
            try:
                if "Content" in data_row.index:
                    content = data_row["Content"]
                else:
                    content = " ".join([str(val) for val in data_row.values])

                score = self.cross_encode([[question, content]])
                output.update({"Rerank_Score": score})
            except:
                reranker_call_success = False

            final_output.append(output)

        # sort this by Rerank_Score score
        if reranker_call_success:
            new_output = sorted(
                final_output, key=lambda x: x["Rerank_Score"], reverse=True
            )
        else:
            new_output = final_output

        # filter to the top x
        new_output = new_output[:result_count]

        return new_output

    def cross_encode(self, pair: List[str]):
        return self.reranker_gaas_model.model(input=pair)

    def init_reranker(self):
        self.reranker_gaas_model = ModelEngine(
            engine_id="30991037-1e73-49f5-99d3-f28210e6b95c12"
        )
