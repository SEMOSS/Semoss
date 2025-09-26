from typing import List, Dict, Union, Optional, Any, Tuple

from datasets import Dataset, concatenate_datasets, disable_caching, Value
import pandas as pd
import faiss
import numpy as np
import pickle
import os
import glob

# CFG/SEMOSS packages
from genai_client import HuggingfaceTokenizer
import gaas_gpt_model as ggm
from ..constants import ENCODING_OPTIONS
from logging_config import get_logger

class FAISSSearcher:
    """
    The primary class for a faiss database classes and searching document embeddings.
    Now supports in-memory metadata join by row id.
    """

    def __init__(
        self,
        embeddings_engine,
        keywords_engine,
        tokenizer,
        metric_type_is_cosine_similarity: bool,
        base_path=None,
        reranker="BAAI/bge-reranker-base",
    ):
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
        master_dataset_path = os.path.join(base_path, "dataset.pkl") if base_path else None
        master_vector_path = os.path.join(base_path, "vectors.pkl") if base_path else None
        if base_path and (not os.path.exists(master_dataset_path) or not os.path.exists(master_vector_path)):
            self.createMasterFiles(self.base_path)

        self.rerank = False
        self.reranker_model = None
        self.reranker_gaas_model = None
        self.reranker_tok = None
        self.reranker = reranker

        disable_caching()
        self.class_logger = get_logger(__name__)

        # Add metadata table for join by id
        self.metadata_table: Dict[str, dict] = {}

    def init_device(self):
        import torch
        self.device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")

    def __getattr__(self, name: str):
        return self.__dict__[f"_{name}"]

    def __setattr__(self, name: str, value: Any):
        if name in ["ds", "embeddings_engine", "keyword_engine", "encoded_vectors", "vector_dimensions", "base_path"]:
            if name == "ds":
                if value is not None and not isinstance(value, (pd.DataFrame, Dataset)):
                    raise TypeError(f"{name} must be a pd.DataFrame or Dataset")
            elif name == "encoded_vectors":
                if value is not None and not isinstance(value, np.ndarray):
                    raise TypeError(f"{name} must be a np.ndarray")
            elif name == "vector_dimensions":
                if value is not None and not isinstance(value, tuple):
                    raise TypeError(f"{name} must be a tuple")
            elif name == "base_path":
                if value is not None and not isinstance(value, str):
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
        for col in columns_to_index:
            text += str(row[col])
            text += separator
        return {target_column: text}

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
    ) -> List[Dict]:
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
        if filter is not None:
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
            final_output = self.do_rerank(
                question=question,
                distances=distances,
                ann_index=ann_index,
                result_count=results,
                columns_to_return=columns_to_return,
                ascending=ascending,
            )
            return final_output
        if self.vector_dimensions[0] < results:
            index_of_minus_one = np.where(ann_index == -1)[0]
            if len(index_of_minus_one) > 0:
                ann_index = ann_index[: index_of_minus_one[0]]
                distances = distances[: index_of_minus_one[0]]
        samples_df = pd.DataFrame({"distances": distances, "ann": ann_index})
        samples_df.sort_values(
            "distances",
            ascending=(ascending if ascending is not None else self.default_sort_direction),
            inplace=True,
        )
        samples_df = samples_df[samples_df["distances"] <= return_threshold]
        final_output = []
        for _, row in samples_df.iterrows():
            output = {}
            output.update({"Score": row["distances"]})
            data_row = self.ds[int(row["ann"])]
            for col in columns_to_return:
                output.update({col: data_row[col]})
            # Metadata merged by doc id (supports both 'ID' and 'id' keys)
            id_val = output.get('ID') or output.get('id')
            if id_val and self.metadata_table and id_val in self.metadata_table:
                output["metadata"] = self.metadata_table[id_val]
            final_output.append(output)
        return final_output

    def add_metadata(self, metadata_dict: Dict[str, dict]):
        """
        Set metadata table for this searcher: {id: metadata dict}
        """
        self.metadata_table = metadata_dict

    def list_documents(self) -> List[str]:
        if self.ds is not None:
            return self.ds.unique("Source")
        return []

    def list_all_records(self) -> List[dict]:
        if self.ds is not None:
            return self.ds.sort(column_names=["Source", "Divider", "Part"]).to_list()
        return []

    def _filter_dataset(self, filter: str) -> List[int]:
        filterDf = self.ds.to_pandas()
        return filterDf.query(filter).index.to_list()

    def load_dataset(self, dataset_location: str) -> None:
        self.ds = self._load_dataset(dataset_location=dataset_location)

    def _load_dataset(self, dataset_location: str) -> Union[Dataset, pd.DataFrame]:
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
                    raise Exception("Unable to read the file with any of the specified encodings")
        elif dataset_location.endswith(".pkl"):
            with open(dataset_location, "rb") as file:
                loaded_dataset = pickle.load(file)
        else:
            raise ValueError("Dataset creation for provided file type has not been defined")
        assert isinstance(loaded_dataset, Dataset)
        dataset_columns = list(loaded_dataset.features)
        extracted_with_cfg = all(
            col in dataset_columns for col in ["Source", "Divider", "Part", "Tokens", "Content"]
        )
        if isinstance(loaded_dataset, Dataset) and extracted_with_cfg:
            if "Modality" not in dataset_columns:
                loaded_dataset = loaded_dataset.add_column(
                    "Modality", ["text" for i in range(loaded_dataset.num_rows)]
                )
            new_features = loaded_dataset.features.copy()
            new_features["Source"] = Value(dtype="string", id=None)
            new_features["Divider"] = Value(dtype="string", id=None)
            new_features["Part"] = Value(dtype="string", id=None)
            new_features["Tokens"] = Value(dtype="int64", id=None)
            new_features["Content"] = Value(dtype="string", id=None)
            try:
                loaded_dataset = loaded_dataset.cast(new_features, keep_in_memory=True)
            except AttributeError:
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
            loaded_dataset["Source"] = loaded_dataset["Source"].astype(str)
            loaded_dataset["Divider"] = loaded_dataset["Divider"].astype(str)
            loaded_dataset["Part"] = loaded_dataset["Part"].astype(str)
            loaded_dataset["Tokens"] = loaded_dataset["Tokens"].astype(int)
            loaded_dataset["Content"] = loaded_dataset["Content"].astype(str)
        return loaded_dataset

    def save_dataset(self, dataset_location: str) -> None:
        with open(dataset_location, "wb") as file:
            pickle.dump(self.ds, file)

    def load_encoded_vectors(self, encoded_vectors_location: str) -> None:
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
        if encoded_vectors_location.endswith(".npy"):
            encoded_vectors = np.load(encoded_vectors_location)
        else:
            with open(encoded_vectors_location, "rb") as file:
                encoded_vectors = pickle.load(file)
        assert isinstance(encoded_vectors, np.ndarray)
        return encoded_vectors

    def save_encoded_vectors(self, encoded_vectors_location: str) -> None:
        with open(encoded_vectors_location, "wb") as file:
            pickle.dump(self.encoded_vectors, file)

    def _concatenate_datasets(
        self,
        datasets: Union[List[Dataset], List[pd.DataFrame]],
    ) -> Union[Dataset, pd.DataFrame]:
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
        assert {os.path.basename(os.path.dirname(path)) for path in documentFileLocation} == {"indexed_files"}
        createDocumentsResponse = {"createdDocuments": []}
        for document in documentFileLocation:
            dataset = self._load_dataset(dataset_location=document)
            sources = dataset.unique("Source")
            for source_name in sources:
                source_dataset = dataset.filter(lambda dataset: dataset["Source"] == source_name)
                directory, base_filename = os.path.split(document)
                new_file_extension = ".pkl"
                if columns_to_index is None or len(columns_to_index) == 0:
                    columns_to_index = list(source_dataset.features)
                new_file_path = os.path.join(
                    directory, source_name + "_dataset" + new_file_extension
                )
                if source_dataset.num_rows > 0:
                    source_dataset = source_dataset.map(
                        self._concatenate_columns,
                        fn_kwargs={
                            "columns_to_index": columns_to_index,
                            "target_column": target_column,
                            "separator": separator,
                        },
                    )
                    if (
                        keyword_search_params is not None
                        and keyword_search_params.pop("keywordSearch", None) is True
                    ):
                        keywords_for_target_col = self.keyword_engine.keyword_extraction(
                            input=source_dataset[target_column],
                            insight_id=insight_id,
                            param_dict=keyword_search_params,
                        )
                        source_dataset = source_dataset.remove_columns(column_names=target_column)
                        source_dataset = source_dataset.add_column(
                            target_column, keywords_for_target_col
                        )
                    vectors = self.embeddings_engine.embeddings(
                        strings_to_embed=source_dataset[target_column],
                        insight_id=insight_id,
                    )
                    vectors = np.array(vectors[0]["response"], dtype=np.float32)
                    assert vectors.ndim == 2
                    columns_to_remove.append(target_column)
                    columns_to_drop = list(set(columns_to_remove).intersection(set(source_dataset.features)))
                    source_dataset = source_dataset.remove_columns(column_names=columns_to_drop)
                    with open(new_file_path, "wb") as file:
                        pickle.dump(source_dataset, file)
                    createDocumentsResponse["createdDocuments"].append(new_file_path)
                    if isinstance(self.tokenizer, HuggingfaceTokenizer):
                        faiss.normalize_L2(vectors)
                    new_file_path = os.path.join(
                        directory, source_name + "_vectors" + new_file_extension,
                    )
                    with open(new_file_path, "wb") as file:
                        pickle.dump(vectors, file)
                    createDocumentsResponse["createdDocuments"].append(new_file_path)
                    if self.encoded_vectors is None:
                        self.encoded_vectors = np.copy(vectors)
                        self.vector_dimensions = self.encoded_vectors.shape
                    else:
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
        created_documents, corrupted_file_sets = self._validateEmbeddingFiles(
            path_to_files=path_to_files,
        )
        return created_documents, corrupted_file_sets

    def _validateEmbeddingFiles(self, path_to_files: str, delete: bool = True) -> Tuple:
        indexed_files_path = os.path.join(path_to_files, "indexed_files")
        all_vector_files = glob.glob(os.path.join(indexed_files_path, "*_vectors.pkl"))
        valid_datasets_and_vectors = []
        corrupted_file_sets: List[Tuple] = []
        created_documents: List[str] = []
        for full_vector_path in all_vector_files:
            vector_filename = os.path.basename(full_vector_path)
            base_filename = vector_filename.split("_vectors.pkl")[0]
            dataset_filename = base_filename + "_dataset.pkl"
            full_dataset_path = os.path.join(indexed_files_path, dataset_filename)
            full_vector_path = os.path.join(indexed_files_path, vector_filename)
            if os.path.exists(full_dataset_path) and os.path.exists(full_vector_path):
                try:
                    dataset = self._load_dataset(dataset_location=full_dataset_path)
                except Exception as e:
                    corrupted_file_sets.append((full_dataset_path, full_vector_path))
                    continue
                try:
                    vectors = self._load_encoded_vectors(
                        encoded_vectors_location=full_vector_path
                    )
                except:
                    corrupted_file_sets.append((full_dataset_path, full_vector_path))
                    continue
                valid_datasets_and_vectors.append((dataset, vectors))
        if len(valid_datasets_and_vectors) > 0:
            self.ds = valid_datasets_and_vectors[0][0]
            self.encoded_vectors = valid_datasets_and_vectors[0][1]
            self.vector_dimensions = self.encoded_vectors.shape
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
            if (self.metric_type_is_cosine_similarity) and (self.vector_dimensions is not None):
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
        corrupted_files = self._validateEmbeddingFiles(
            path_to_files=path_to_files,
        )[1]
        return corrupted_files

    def datasetsLoaded(self) -> bool:
        if (
            (self.ds is None)
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
        if self.reranker_gaas_model is None:
            self.init_reranker()
        samples_df = pd.DataFrame({"distances": distances, "ann": ann_index})
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
                output.update({col: data_row[col]})
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
        if reranker_call_success:
            new_output = sorted(final_output, key=lambda x: x["Sim"], reverse=True)
        else:
            new_output = final_output
        new_output = new_output[:result_count]
        return new_output

    def cross_encode(self, pair: List[str]):
        return self.reranker_gaas_model.model(input=pair)

    def init_reranker(self):
        self.reranker_gaas_model = ggm.ModelEngine(
            engine_id="30991037-1e73-49f5-99d3-f28210e6b95c12"
        )
