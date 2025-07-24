from typing import List, Dict, Union, Optional, Any, Tuple
from datasets import Dataset, concatenate_datasets, disable_caching, Value

import pandas as pd
import faiss
import numpy as np
import pickle
import os
import glob
import json
import bm25s

# CFG/SEMOSS packages
from genai_client import HuggingfaceTokenizer
import gaas_gpt_model as ggm
from ..constants import ENCODING_OPTIONS
from logging_config import get_logger


class FAISSSearcher:
    """
    The primary class for a faiss database classes and searching document embeddings
    """

    def __init__(
        self,
        embeddings_engine,
        keywords_engine,
        tokenizer,
        metric_type_is_cosine_similarity: bool,
        base_path=None,
        reranker="BAAI/bge-reranker-base",
        enable_hybrid_search: bool = True,
    ):
        self.class_logger = get_logger(__name__)
        self.init_device()
        self.ds = None

        self.encoded_vectors = None
        self.vector_dimensions = None

        self.embeddings_engine = embeddings_engine
        self.keyword_engine = keywords_engine

        self.tokenizer = tokenizer

        self.metric_type_is_cosine_similarity = metric_type_is_cosine_similarity
        self.default_sort_direction = (
            False if self.metric_type_is_cosine_similarity else True
        )

        self.base_path = base_path

        # BM25 components
        self.enable_hybrid_search = enable_hybrid_search
        self.bm25_index = None
        self.bm25_corpus = None

        # File paths for BM25 storage
        if base_path:
            self.bm25_index_path = os.path.join(base_path, "bm25_index")
            self.bm25_corpus_path = os.path.join(base_path, "bm25_corpus.json")

        # Load existing files
        master_dataset_path = os.path.join(base_path, "dataset.pkl")
        master_vector_path = os.path.join(base_path, "vectors.pkl")

        if not os.path.exists(master_dataset_path) or not os.path.exists(
            master_vector_path
        ):
            self.createMasterFiles(self.base_path)

        # Load BM25 index if it exists
        if self.enable_hybrid_search:
            self._load_bm25_index()

        self.rerank = False
        self.reranker_model = None
        self.reranker_gaas_model = None
        self.reranker_tok = None
        self.reranker = reranker

        disable_caching()

    def __getattr__(self, name: str):
        """Retrieve attribute from object's dictionary."""
        return self.__dict__[f"_{name}"]

    def __setattr__(self, name: str, value: Any):
        """
        Assign a value to a named attribute and enforce correct data type before assignment.
        """
        if name == "encoded_vectors" or value != None:
            if name in ["ds"]:
                if not isinstance(value, (pd.DataFrame, Dataset)):
                    raise TypeError(f"{name} must be a pd.DataFrame or Dataset")
            elif name in ["embeddings_engine", "keyword_engine"]:
                pass
                # if not isinstance(value, EncoderInterface):
                #       raise TypeError(f"{name} must be an instance of EncoderInterface")
            elif name in ["encoded_vectors"]:
                if (np.any(value) != None) and not isinstance(value, np.ndarray):
                    raise TypeError(f"{name} must be a np.ndarray")
            elif name in ["vector_dimensions"]:
                if not isinstance(value, tuple):
                    raise TypeError(f"{name} must be a tuple")
            elif name in ["base_path"]:
                if not isinstance(value, str):
                    raise TypeError(f"{name} must be a string")

        self.__dict__[f"_{name}"] = value

    def _concatenate_columns(
        self,
        row: Dict[str, Any],
        target_column: str,
        columns_to_index: List[str] = None,
        separator: str = "\n",
    ) -> Dict[str, str]:
        text = ""
        """
        Given a set of Index Classes, find the closest match(es) using FAISSearcher.nearestNeighbor across all index classes.

        Args:
            row (`Dict[str, Any]`): A row dictionary in a dataset
            results (`Optional[Union[int, None]]`): The column name or key for the concatenated column values
            columns_to_index (`List[str]`): A list containing the column names to be concatenated
            separator (`str`): The value to separate the concatenated values by
        
        Return:
            `Dict[str, str]` A dictionary containing the new column name as the key and the concatenated columns as a the value.
        """
        for col in columns_to_index:
            text += str(row[col])
            text += separator

        return {target_column: text}

    def init_device(self):
        """
        Utility method to determine whether or not the devie running the interpreter has a gpu
        """
        import torch

        self.device = (
            torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
        )

    def _load_bm25_index(self):
        """Load existing BM25 index and corpus if they exist"""
        try:
            if os.path.exists(self.bm25_index_path) and os.path.exists(
                self.bm25_corpus_path
            ):
                self.bm25_index = bm25s.BM25.load(
                    self.bm25_index_path, load_corpus=True
                )

                with open(self.bm25_corpus_path, "r", encoding="utf-8") as f:
                    self.bm25_corpus = json.load(f)

        except Exception as e:
            self.class_logger.warning(f"Failed to load BM25 index: {e}")
            self.bm25_index = None
            self.bm25_corpus = None

    def _save_bm25_index(self):
        """Save BM25 index and corpus to disk"""
        try:
            if self.bm25_index is not None:
                self.bm25_index.save(self.bm25_index_path, corpus=self.bm25_corpus)

                with open(self.bm25_corpus_path, "w", encoding="utf-8") as f:
                    json.dump(self.bm25_corpus, f, ensure_ascii=False, indent=2)

        except Exception as e:
            self.class_logger.error(f"Failed to save BM25 index: {e}")

    def _build_bm25_index(self, texts: List[str]):
        """Build BM25 index from text corpus"""
        try:
            corpus_with_metadata = []
            for i, text in enumerate(texts):
                corpus_with_metadata.append({"id": i, "text": text})

            text_only = [item["text"] for item in corpus_with_metadata]
            corpus_tokens = bm25s.tokenize(text_only, stopwords="en")

            self.bm25_index = bm25s.BM25(method="lucene")

            self.bm25_index.index(corpus_tokens)

            self.bm25_corpus = texts

            self._save_bm25_index()

        except Exception as e:
            self.class_logger.error(f"Failed to build BM25 index: {e}")
            self.bm25_index = None
            self.bm25_corpus = None

    def _update_bm25_index(self, new_texts: List[str]):
        """Update BM25 index with new texts"""
        try:
            if self.bm25_corpus is None:
                self.bm25_corpus = []

            all_texts = self.bm25_corpus + new_texts

            self._build_bm25_index(all_texts)

        except Exception as e:
            self.class_logger.error(f"Failed to update BM25 index: {e}")

    def _bm25_search(self, query: str, top_k: int = 10) -> List[Tuple[int, float]]:
        """Perform BM25 search and return document indices with scores"""
        if self.bm25_index is None or self.bm25_corpus is None:
            return []

        try:
            query_tokens = bm25s.tokenize([query], stopwords="en")

            if top_k > len(self.bm25_corpus):
                top_k = len(self.bm25_corpus)

            scores, indices = self.bm25_index.retrieve(query_tokens, k=top_k)

            results = []
            for i, (score_array, idx_array) in enumerate(zip(scores, indices)):
                for score, idx in zip(score_array, idx_array):
                    if isinstance(score, dict) and "id" in score:
                        doc_index = score["id"]
                        similarity_score = float(idx)

                        if doc_index != -1:
                            results.append((int(doc_index), similarity_score))
                    else:
                        self.class_logger.warning(
                            f"Unexpected BM25 result format - score: {type(score)}, idx: {type(idx)}"
                        )
                        if idx != -1:
                            results.append(
                                (
                                    int(idx),
                                    (
                                        float(score)
                                        if not isinstance(score, dict)
                                        else 0.0
                                    ),
                                )
                            )

            results.sort(key=lambda x: x[1], reverse=True)

            return results

        except Exception as e:
            self.class_logger.error(f"BM25 search failed: {e}")
            return []

    def _reciprocal_rank_fusion(
        self,
        vector_results: List[Dict],
        bm25_results: List[Tuple[int, float]],
        k: int = 60,
    ) -> List[Dict]:
        """
        Combine vector and BM25 results using Reciprocal Rank Fusion

        Args:
            vector_results: Results from vector search with 'Score' and data
            bm25_results: Results from BM25 search as (index, score) tuples
            k: RRF parameter (typically 60)
        """
        vector_score_map = {}
        for i, result in enumerate(vector_results):
            vector_score_map[i] = {
                "rrf_score": 1.0 / (k + i + 1),  # RRF formula
                "result": result,
            }

        bm25_score_map = {}
        for rank, (doc_idx, score) in enumerate(bm25_results):
            if doc_idx < len(self.ds):
                bm25_score_map[doc_idx] = {
                    "rrf_score": 1.0 / (k + rank + 1),
                    "bm25_score": score,
                    "doc_idx": doc_idx,
                }

        combined_scores = {}

        for idx, data in vector_score_map.items():
            combined_scores[idx] = {
                "rrf_score": data["rrf_score"],
                "result": data["result"],
                "vector_score": data["result"]["Score"],
                "bm25_score": 0.0,
            }

        for doc_idx, data in bm25_score_map.items():
            if doc_idx in combined_scores:
                combined_scores[doc_idx]["rrf_score"] += data["rrf_score"]
                combined_scores[doc_idx]["bm25_score"] = data["bm25_score"]
            else:
                if doc_idx < len(self.ds):
                    data_row = self.ds[doc_idx]
                    result = {"Score": float("inf")}  # High distance for vector search
                    for col in data_row.keys():
                        result[col] = data_row[col]

                    combined_scores[doc_idx] = {
                        "rrf_score": data["rrf_score"],
                        "result": result,
                        "vector_score": float("inf"),
                        "bm25_score": data["bm25_score"],
                    }

        sorted_results = sorted(
            combined_scores.items(), key=lambda x: x[1]["rrf_score"], reverse=True
        )

        final_results = []
        for idx, data in sorted_results:
            result = data["result"].copy()
            result["RRF_Score"] = data["rrf_score"]
            result["BM25_Score"] = data["bm25_score"]
            result["Vector_Score"] = data["vector_score"]
            final_results.append(result)

        return final_results

    def nearestNeighbor(
        self,
        question: str,
        filter: Optional[str] = None,
        results: Optional[int] = 5,
        columns_to_return: Optional[List[str]] = None,
        return_threshold: Optional[Union[int, float]] = 1000,
        ascending: Optional[bool] = None,
        total_results: Optional[int] = 10,
        insight_id: Optional[str] = None,
        use_hybrid_search: Optional[bool] = None,
    ) -> List[Dict]:
        """
        Enhanced nearest neighbor search with optional hybrid BM25 + vector search
        """
        use_hybrid = (
            use_hybrid_search
            if use_hybrid_search is not None
            else self.enable_hybrid_search
        )

        if not use_hybrid or self.bm25_index is None:
            # if im not hybrid im using original vector-only search
            return self._vector_only_search(
                question,
                filter,
                results,
                columns_to_return,
                return_threshold,
                ascending,
                total_results,
                insight_id,
            )

        return self._hybrid_search(
            question,
            filter,
            results,
            columns_to_return,
            return_threshold,
            ascending,
            total_results,
            insight_id,
        )

    def _hybrid_search(
        self,
        question,
        filter,
        results,
        columns_to_return,
        return_threshold,
        ascending,
        total_results,
        insight_id,
    ):
        """Perform hybrid BM25 + vector search"""
        if columns_to_return is None:
            columns_to_return = list(self.ds.features)

        fusion_results = max(total_results * 2, 20)

        # 1. Do vector search
        vector_results = self._vector_only_search(
            question,
            filter,
            fusion_results,
            columns_to_return,
            return_threshold,
            ascending,
            fusion_results,
            insight_id,
        )

        # 2. Do BM25 search
        bm25_results = self._bm25_search(question, top_k=fusion_results)

        # 3. Combine using reciprocal rank fusion
        if bm25_results:
            hybrid_results = self._reciprocal_rank_fusion(vector_results, bm25_results)
        else:
            # fall back to vector-only if BM25 failed
            hybrid_results = vector_results

        # 4. return top results
        return hybrid_results[:results]

    def _vector_only_search(
        self,
        question,
        filter,
        results,
        columns_to_return,
        return_threshold,
        ascending,
        total_results,
        insight_id,
    ):
        """Original vector-only search logic"""
        if columns_to_return is None:
            columns_to_return = list(self.ds.features)

        search_vector = self.embeddings_engine.embeddings(
            strings_to_embed=[question], insight_id=insight_id
        )

        if isinstance(search_vector, List):
            query_vector = np.array(search_vector[0]["response"], dtype=np.float32)
        else:
            query_vector = np.array(search_vector["response"], dtype=np.float32)
        assert query_vector.shape[0] == 1

        if isinstance(self.tokenizer, HuggingfaceTokenizer):
            faiss.normalize_L2(query_vector)

        if not isinstance(results, int):
            results = int(results)

        if not self.rerank:
            total_results = results

        if filter != None:
            filter_ids = self._filter_dataset(filter)
            id_selector = faiss.IDSelectorArray(filter_ids)
            distances, ann_index = self.index.search(
                query_vector,
                k=total_results,
                params=faiss.SearchParametersIVF(sel=id_selector),
            )
        else:
            distances, ann_index = self.index.search(query_vector, k=total_results)

        distances = distances[0]
        ann_index = ann_index[0]

        if self.rerank:
            return self.do_rerank(
                question=question,
                distances=distances,
                ann_index=ann_index,
                result_count=results,
                columns_to_return=columns_to_return,
                ascending=ascending,
            )

        if self.vector_dimensions[0] < results:
            index_of_minus_one = np.where(ann_index == -1)[0]
            if len(index_of_minus_one) > 0:
                ann_index = ann_index[: index_of_minus_one[0]]
                distances = distances[: index_of_minus_one[0]]

        samples_df = pd.DataFrame({"distances": distances, "ann": ann_index})
        samples_df.sort_values(
            "distances",
            ascending=(
                ascending if ascending is not None else self.default_sort_direction
            ),
            inplace=True,
        )
        samples_df = samples_df[samples_df["distances"] <= return_threshold]

        final_output = []
        for _, row in samples_df.iterrows():
            output = {"Score": row["distances"]}
            data_row = self.ds[int(row["ann"])]
            for col in columns_to_return:
                output[col] = data_row[col]
            final_output.append(output)

        return final_output

    def list_documents(self) -> List[str]:
        """
        Get the unique list of documents
        """
        if self.ds is not None:
            return self.ds.unique("Source")

        return []

    def list_all_records(self) -> List[dict]:
        """
        Get the list of all the records
        """
        if self.ds is not None:
            return self.ds.sort(column_names=["Source", "Divider", "Part"]).to_list()

        return []

    def _filter_dataset(self, filter: str) -> List[int]:
        filterDf = self.ds.to_pandas()

        return filterDf.query(filter).index.to_list()

    def load_dataset(self, dataset_location: str) -> None:
        """
        Utility method to load stored datasets into the object.

        Args:
        dataset_location(`str`):
            The file path to the stored dataset. Currently only csv and pkl file types are supported

        Returns:
        `None`
        """
        self.ds = self._load_dataset(dataset_location=dataset_location)

    def _load_dataset(self, dataset_location: str) -> Union[Dataset, pd.DataFrame]:
        """
        Internal method to load the dataset based on its file type.

        Args:
        dataset_location(`str`):
            The file path to the stored dataset. Currently only csv and pkl file types are supported

        Returns:
        `None`
        """
        if dataset_location.endswith(".csv"):
            try:
                loaded_dataset = Dataset.from_csv(
                    path_or_paths=dataset_location,
                    encoding="utf-8",
                    keep_in_memory=True,
                )
            except:
                for encoding in ENCODING_OPTIONS:
                    try:
                        temp_df = pd.read_csv(dataset_location, encoding=encoding)
                        loaded_dataset = Dataset.from_pandas(temp_df)
                        break
                    except:
                        continue
                else:
                    # The else clause is executed if the loop completes without encountering a break
                    raise Exception(
                        "Unable to read the file with any of the specified encodings"
                    )

        elif dataset_location.endswith(".pkl"):
            with open(dataset_location, "rb") as file:
                loaded_dataset = pickle.load(file)
        else:
            raise ValueError(
                "Dataset creation for provided file type has not been defined"
            )

        assert isinstance(loaded_dataset, Dataset)

        dataset_columns = list(loaded_dataset.features)

        extracted_with_cfg = all(
            col in dataset_columns
            for col in ["Source", "Divider", "Part", "Tokens", "Content"]
        )
        if isinstance(loaded_dataset, Dataset) and extracted_with_cfg:

            if "Modality" not in dataset_columns:
                loaded_dataset = loaded_dataset.add_column(
                    "Modality", ["text" for i in range(loaded_dataset.num_rows)]
                )

            # to be safe, force all columns
            new_features = loaded_dataset.features.copy()
            new_features["Source"] = Value(dtype="string", id=None)
            new_features["Divider"] = Value(dtype="string", id=None)
            new_features["Part"] = Value(dtype="string", id=None)
            new_features["Tokens"] = Value(dtype="int64", id=None)
            new_features["Content"] = Value(dtype="string", id=None)

            try:
                loaded_dataset = loaded_dataset.cast(new_features, keep_in_memory=True)
            except AttributeError:
                # This catch is required due to a version change in the datasets package
                # Previously, there was no attribute called _batches which is required with the new `cast` method. This is missing from the pickle file
                # The solution is to reconstruct the dataset from a pandas frame
                try:
                    loaded_dataset = Dataset.from_pandas(loaded_dataset.to_pandas())
                except AttributeError:
                    loaded_dataset = Dataset.from_pandas(
                        loaded_dataset.data.to_pandas()
                    )
                loaded_dataset = loaded_dataset.cast(new_features, keep_in_memory=True)

        elif isinstance(loaded_dataset, pd.DataFrame) and extracted_with_cfg:
            if "Modality" not in dataset_columns:
                loaded_dataset["Modality"] = "text"

            # to be safe, force all columns
            loaded_dataset["Source"] = loaded_dataset["Source"].astype(str)
            loaded_dataset["Divider"] = loaded_dataset["Divider"].astype(str)
            loaded_dataset["Part"] = loaded_dataset["Part"].astype(str)
            loaded_dataset["Tokens"] = loaded_dataset["Tokens"].astype(int)
            loaded_dataset["Content"] = loaded_dataset["Content"].astype(str)

        return loaded_dataset

    def save_dataset(self, dataset_location: str) -> None:
        """
        Utility method to save datasets from object onto the disk.

        Args:
        dataset_location(`str`):
            The file path to the write the dataset.

        Returns:
        `None`
        """
        with open(dataset_location, "wb") as file:
            pickle.dump(self.ds, file)

    def load_encoded_vectors(self, encoded_vectors_location: str) -> None:
        """
        Utility method to load stored embeddings from the disk.

        Args:
        encoded_vectors_location(`str`):
            The file path to the stored embeddings file. Currently only npy and pkl file types are supported

        Returns:
        `None`
        """
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
            The file path to the stored embeddings file. Currently only npy and pkl file types are supported

        Returns:
        `None`
        """
        if encoded_vectors_location.endswith(".npy"):
            encoded_vectors = np.load(encoded_vectors_location)
        else:
            with open(encoded_vectors_location, "rb") as file:
                encoded_vectors = pickle.load(file)

        assert isinstance(encoded_vectors, np.ndarray)

        return encoded_vectors

    def save_encoded_vectors(self, encoded_vectors_location: str) -> None:
        """
        Utility method to save embeddings from object onto the disk.

        Args:
        encoded_vectors_location(`str`):
            The file path to the write the dataset.

        Returns:
        `None`
        """
        with open(encoded_vectors_location, "wb") as file:
            pickle.dump(self.encoded_vectors, file)

    def _concatenate_datasets(
        self,
        datasets: Union[List[Dataset], List[pd.DataFrame]],
    ) -> Union[Dataset, pd.DataFrame]:
        """
        Interal utility method to concatenate datasets depending on the class type. Either pandas.DataFrame or datasets.Dataset

        Args:
        datasets(`Union[List[Dataset], List[pd.DataFrame]]`):
            A list of datasets where all the datasets of only of one type. Either pandas.DataFrame or datasets.Dataset

        Returns:
        `Union[Dataset, pd.DataFrame]`
        """
        return concatenate_datasets(datasets)

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
                if "Content" in self.ds.features:
                    all_texts = self.ds["Content"]
                else:
                    # Fallback: concatenate all text fields
                    all_texts = []
                    for i in range(len(self.ds)):
                        row = self.ds[i]
                        text_parts = []
                        for key, value in row.items():
                            if isinstance(value, str):
                                text_parts.append(value)
                        all_texts.append(" ".join(text_parts))

                if self.bm25_index is None:
                    self._build_bm25_index(all_texts)
                else:
                    # eventually i want to build incrementally here..
                    self._build_bm25_index(all_texts)

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
            `Dict` A dictionary listing which documents have been successfully created
        """
        # make sure they are all in indexed_files dir
        assert {
            os.path.basename(os.path.dirname(path)) for path in documentFileLocation
        } == {"indexed_files"}

        # create a list of the documents created so that we can push the files back to the cloud
        createDocumentsResponse = {
            "createdDocuments": [],
        }

        # loop through and embed new docs
        for document in documentFileLocation:
            # Create the Dataset for every file
            dataset = self._load_dataset(dataset_location=document)

            # Change the unit of work
            # From being the document
            # To the individual source inside the document
            sources = dataset.unique("Source")
            for source_name in sources:
                source_dataset = dataset.filter(
                    lambda dataset: dataset["Source"] == source_name
                )

                # Get the directory path and the base filename without extension
                directory, base_filename = os.path.split(document)
                new_file_extension = ".pkl"

                if columns_to_index == None or len(columns_to_index) == 0:
                    columns_to_index = list(source_dataset.features)

                # save the dataset, this is for efficiency after removing docs
                new_file_path = os.path.join(
                    directory, source_name + "_dataset" + new_file_extension
                )

                # if applicable, create the concatenated columns
                if source_dataset.num_rows > 0:
                    source_dataset = source_dataset.map(
                        self._concatenate_columns,
                        fn_kwargs={
                            "columns_to_index": columns_to_index,
                            "target_column": target_column,
                            "separator": separator,
                        },
                    )

                    # transform chunks into keywords
                    if (
                        keyword_search_params != None
                        and keyword_search_params.pop("keywordSearch", None) is True
                    ):
                        keywords_for_target_col = (
                            self.keyword_engine.keyword_extraction(
                                input=source_dataset[target_column],
                                insight_id=insight_id,
                                param_dict=keyword_search_params,
                            )
                        )
                        # source_dataset = source_dataset.add_column(target_column, keywords_for_target_col)
                        source_dataset = source_dataset.remove_columns(
                            column_names=target_column
                        )
                        source_dataset = source_dataset.add_column(
                            target_column, keywords_for_target_col
                        )

                    # get the embeddings for the document
                    # vectors = self.embeddings_engine.get_embeddings(dataset[target_column])
                    vectors = self.embeddings_engine.embeddings(
                        strings_to_embed=source_dataset[target_column],
                        insight_id=insight_id,
                    )
                    vectors = np.array(vectors[0]["response"], dtype=np.float32)
                    assert vectors.ndim == 2

                    columns_to_remove.append(target_column)
                    columns_to_drop = list(
                        set(columns_to_remove).intersection(
                            set(source_dataset.features)
                        )
                    )
                    source_dataset = source_dataset.remove_columns(
                        column_names=columns_to_drop
                    )

                    with open(new_file_path, "wb") as file:
                        pickle.dump(source_dataset, file)

                    # add the created source_dataset file path
                    createDocumentsResponse["createdDocuments"].append(new_file_path)

                    # normalize the vectors if using huggingface
                    if isinstance(self.tokenizer, HuggingfaceTokenizer):
                        faiss.normalize_L2(vectors)

                    # write out the vectors with the same file name
                    # Change the file extension to ".pkl"
                    new_file_path = os.path.join(
                        directory,
                        source_name + "_vectors" + new_file_extension,
                    )
                    with open(new_file_path, "wb") as file:
                        pickle.dump(vectors, file)

                    # add the created embeddings file path
                    createDocumentsResponse["createdDocuments"].append(new_file_path)

                    # TODO need to update the flow for how we instatiate
                    if np.any(self.encoded_vectors) == None:
                        self.encoded_vectors = np.copy(vectors)
                        self.vector_dimensions = self.encoded_vectors.shape
                    else:
                        # make sure the dimensions are the same
                        assert self.vector_dimensions[1] == vectors.shape[1]
                        self.encoded_vectors = np.concatenate(
                            [self.encoded_vectors, vectors], axis=0
                        )

        master_indexClass_files, corrupted_file_sets = self.createMasterFiles(
            path_to_files=os.path.dirname(os.path.dirname(documentFileLocation[0]))
        )

        for corrupted_set in corrupted_file_sets:
            for file_path in corrupted_set:
                createDocumentsResponse["createdDocuments"].remove(file_path)

        createDocumentsResponse["createdDocuments"].extend(master_indexClass_files)

        return createDocumentsResponse

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

        # List all pdfs files in the directory
        all_vector_files = glob.glob(os.path.join(indexed_files_path, "*_vectors.pkl"))

        valid_datasets_and_vectors = []
        corrupted_file_sets: List[Tuple] = []
        created_documents: List[str] = []

        for full_vector_path in all_vector_files:
            # get the basename of the file
            # all csvs, datasets and vectors should contain this base name
            vector_filename = os.path.basename(full_vector_path)
            base_filename = vector_filename.split("_vectors.pkl")[0]
            dataset_filename = base_filename + "_dataset.pkl"

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
            self.ds = valid_datasets_and_vectors[0][0]
            self.encoded_vectors = valid_datasets_and_vectors[0][1]
            self.vector_dimensions = self.encoded_vectors.shape

            # loop through and concatenate the others if any
            for dataset, vectors in valid_datasets_and_vectors[1:]:
                self.ds = self._concatenate_datasets([self.ds, dataset])
                self.encoded_vectors = np.concatenate(
                    (self.encoded_vectors, vectors), axis=0
                )

            encoded_vectors_location = os.path.join(path_to_files, "vectors.pkl")
            dataset_location = os.path.join(path_to_files, "dataset.pkl")
            self.save_encoded_vectors(encoded_vectors_location=encoded_vectors_location)
            self.save_dataset(dataset_location=dataset_location)
            created_documents.append(encoded_vectors_location)
            created_documents.append(dataset_location)

            if (self.metric_type_is_cosine_similarity) and (
                self.vector_dimensions != None
            ):
                self.index = faiss.index_factory(
                    self.vector_dimensions[1], "Flat", faiss.METRIC_INNER_PRODUCT
                )
            elif self.vector_dimensions != None:
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
        if (
            (self.ds == None)
            or (list(self.ds.features) == [])
            or (len(list(self.ds.features)) == 0)
            or (self.ds.num_rows == 0)
        ):
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
        ascending: Optional[bool] = None,
    ):
        # reranks based on an algorithm and then finds

        if self.reranker_gaas_model is None:
            self.init_reranker()

        samples_df = pd.DataFrame({"distances": distances, "ann": ann_index})

        # samples_df.sort_values(
        #    "distances",
        #    ascending = (ascending if ascending is not None else self.default_sort_direction),
        #    inplace=True
        # )
        # samples_df = samples_df[samples_df['distances'] <= return_threshold]
        # self.class_logger.warning(f"Return length is set to {len(distances)}", extra={"stack": "BACKEND"})

        # create the response payload by adding the relevant columns from the dataset
        result_chunks = []

        # see if rerank is enabled
        # if so run through reranking this
        # and then limit to the final result
        final_output = []

        reranker_call_success = True
        for _, row in samples_df.iterrows():
            output = {}
            output.update({"Score": row["distances"]})
            data_row = self.ds[int(row["ann"])]

            self.class_logger.info(
                f"Row to pick {int(row['ann'])}", extra={"stack": "BACKEND"}
            )
            self.class_logger.info(
                f"[{str(data_row['Content'])}]", extra={"stack": "BACKEND"}
            )

            for col in columns_to_return:
                # self.class_logger.warning(f"{col} {data_row[col]}", extra={"stack": "BACKEND"})
                output.update({col: data_row[col]})

            # this is not pythonic but let us try this for now
            # self.class_logger.warning(question, extra={"stack": "BACKEND"})
            try:
                if "Content" in data_row.keys():
                    content = data_row["Content"]
                else:
                    content = " ".join([str(val) for val in data_row.values()])

                score = self.cross_encode([[question, content]])

                output.update({"Sim": score})
            except:
                reranker_call_success = False

            final_output.append(output)

        # sort this by sim score
        if reranker_call_success:
            new_output = sorted(final_output, key=lambda x: x["Sim"], reverse=True)
        else:
            new_output = final_output

        # filter to the top x
        new_output = new_output[:result_count]

        return new_output

        # now comes the reranker

    def cross_encode(self, pair: List[str]):
        return self.reranker_gaas_model.model(input=pair)

    def init_reranker(self):
        self.reranker_gaas_model = ggm.ModelEngine(
            engine_id="30991037-1e73-49f5-99d3-f28210e6b95c12"
        )
