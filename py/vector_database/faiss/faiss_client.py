from typing import (
    List, 
    Dict, 
    Union, 
    Optional, 
    Any, 
    Tuple
)
from datasets import (
    Dataset, 
    concatenate_datasets, 
    load_dataset, 
    disable_caching, 
    Value
)

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

class FAISSSearcher():
    '''
    The primary class for a faiss database classes and searching document embeddings
    '''
    
    def __init__(
        self, 
        embeddings_engine,
        keywords_engine,
        tokenizer,
        metric_type_is_cosine_similarity:bool,
        base_path = None,
        reranker="BAAI/bge-reranker-base"
    ):
        self.init_device()
        self.ds = None

        self.encoded_vectors = None
        self.vector_dimensions = None

        self.embeddings_engine = embeddings_engine
        self.keyword_engine = keywords_engine
        
        self.tokenizer = tokenizer

        self.base_path = base_path

        self.metric_type_is_cosine_similarity = metric_type_is_cosine_similarity
        self.default_sort_direction = False if self.metric_type_is_cosine_similarity else True
               
        self.rerank = False                      # disable reranking by default
        self.reranker_model = None
        self.reranker_gaas_model = None
        self.reranker_tok = None
        self.reranker = reranker
        
       
        disable_caching()                        # disable caching within the shell so that engines can be exported
        
        self.class_logger = get_logger(__name__)

    def __getattr__(self, name: str):
        '''Retrieve attribute from object's dictionary.'''
        return self.__dict__[f"_{name}"]
  
    def __setattr__(self, name:str, value:Any):
        '''
        Assign a value to a named attribute and enforce correct data type before assignment.
        '''
        if name == 'encoded_vectors' or value != None:
            if name in ['ds']:
                if not isinstance(value, (pd.DataFrame, Dataset)):
                    raise TypeError(f"{name} must be a pd.DataFrame or Dataset")
            elif name in ['embeddings_engine', 'keyword_engine']:
                pass
                # if not isinstance(value, EncoderInterface):
                #       raise TypeError(f"{name} must be an instance of EncoderInterface")
            elif name in ['encoded_vectors']:
                if (np.any(value) != None) and not isinstance(value, np.ndarray) :
                    raise TypeError(f"{name} must be a np.ndarray")
            elif name in ['vector_dimensions']:
                if not isinstance(value, tuple):
                    raise TypeError(f"{name} must be a tuple")         
            elif name in ['base_path']:
                if not isinstance(value, str):
                    raise TypeError(f"{name} must be a string")
          
        self.__dict__[f"_{name}"] = value
   
    def _concatenate_columns(
        self, 
        row:Dict[str, Any], 
        target_column:str, 
        columns_to_index:List[str] = None,
        separator:str = "\n"
    ) -> Dict[str, str]:
        text = ""
        '''
        Given a set of Index Classes, find the closest match(es) using FAISSearcher.nearestNeighbor across all index classes.

        Args:
            row (`Dict[str, Any]`): A row dictionary in a dataset
            results (`Optional[Union[int, None]]`): The column name or key for the concatenated column values
            columns_to_index (`List[str]`): A list containing the column names to be concatenated
            separator (`str`): The value to separate the concatenated values by
        
        Return:
            `Dict[str, str]` A dictionary containing the new column name as the key and the concatenated columns as a the value.
        '''
        for col in columns_to_index:
            text += str(row[col])
            text += separator
            
        return {target_column : text}
    
    def init_device(self):
        '''
        Utility method to determine whether or not the devie running the interpreter has a gpu
        '''
        import torch
        self.device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
    
    def nearestNeighbor(
        self, 
        question: str,
        filter: Optional[str] = None,
        results: Optional[int] = 5, 
        columns_to_return: Optional[List[str]] = None, 
        return_threshold: Optional[Union[int,float]] = 1000, 
        ascending : Optional[bool] = None,
        total_results: Optional[int] = 10,                       # this is used for reranking
        insight_id:Optional[str] = None,
    ) -> List[Dict]:
        '''
        Find the closest match(es) between the question bassed in and the embedded documents using Euclidena Distance.

        Args:
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
            >>> faissSearcherObj.nearestNeighbor(
            ...     question="""How is the president chosen""",
            ...     results = 3,
            ...     columns_to_return = ['doc_index'],
            ...     return_threshold = 1.0,
            ...     ascending = False
            ... )
            [{Score=0.9867115616798401, doc_index=1420-deloitte-independence_11_text}, 
            {Score=0.9855965375900269, doc_index=1420-deloitte-independence_10_text}]
        '''
        # if columns_to_return is None, then by default we return all columns
        if(columns_to_return is None):
            columns_to_return = list(self.ds.features)

        search_vector = self.embeddings_engine.embeddings(
            strings_to_embed = [question], 
            insight_id = insight_id
        )
        
        query_vector = np.array(search_vector[0]['response'], dtype=np.float32)
        assert query_vector.shape[0] == 1

        # check to see if need to normalize the vector
        if isinstance(self.tokenizer, HuggingfaceTokenizer):
            faiss.normalize_L2(query_vector)

        # perform the faiss search. Scores returned are Euclidean distances
        # distances - the measurement score between the embedded question and the Approximate Nearest Neighbor (ANN)
        # ann_index - the index location of the Approximate Nearest Neighbor (ANN)

        if not isinstance(results, int):
            results = int(results)
            
        if not self.rerank:
          total_results = results
        
        # If a filter was passed in then we need to get the indexes
        if filter != None:
            filter_ids = self._filter_dataset(filter)
            id_selector = faiss.IDSelectorArray(filter_ids)
            distances, ann_index = self.index.search(
                query_vector, 
                k = total_results, 
                params=faiss.SearchParametersIVF(sel=id_selector)
            )
        else:
            distances, ann_index = self.index.search( 
                query_vector, 
                k = total_results
            )

        distances = distances[0]
        ann_index = ann_index[0]

        if self.rerank:
            final_output = self.do_rerank(
                question=question, 
                distances=distances, 
                ann_index=ann_index, 
                result_count=results, 
                columns_to_return=columns_to_return, 
                ascending=ascending
            )
            
            return final_output

    
        # this is a safety check to make sure we are only returning good vectors if the limit was too high
        if self.vector_dimensions[0] < results:
            # Find the index of the first occurrence of -1
            index_of_minus_one = np.where(ann_index == -1)[0]
            # If -1 is not found, index_of_minus_one will be an empty array
            # In that case, we keep the original array, otherwise, we slice it
            if len(index_of_minus_one) > 0:
                ann_index = ann_index[:index_of_minus_one[0]]
                distances = distances[:index_of_minus_one[0]]

        # create the return data
        samples_df = pd.DataFrame(
            {
                'distances': distances, 
                'ann': ann_index
            }
        )
           
        samples_df.sort_values(
            "distances", 
            ascending = (ascending if ascending is not None else self.default_sort_direction), 
            inplace=True
        )
        samples_df = samples_df[samples_df['distances'] <= return_threshold]
      
        # create the response payload by adding the relevant columns from the dataset
        final_output = []
        
        # see if rerank is enabled
        # if so run through reranking this
        # and then limit to the final result 
        
        for _, row in samples_df.iterrows():
            output = {}
            output.update({'Score' : row['distances']})
            data_row = self.ds[int(row['ann'])]
            for col in columns_to_return:
                output.update({col:data_row[col]})
            final_output.append(output)
    
        return final_output
    
    def _filter_dataset(self, filter:str) -> List[int]:
        filterDf = self.ds.to_pandas()
        
        return filterDf.query(filter).index.to_list()

    def load_dataset(
        self, 
        dataset_location:str
    ) -> None:
        '''
        Utility method to load stored datasets into the object. 

        Args:
        dataset_location(`str`):
            The file path to the stored dataset. Currently only csv and pkl file types are supported

        Returns:
        `None`
        '''
        self.ds = self._load_dataset(dataset_location = dataset_location)

    def _load_dataset(
        self, 
        dataset_location:str
    ) -> Union[Dataset, pd.DataFrame]:
        '''
        Internal method to load the dataset based on its file type.

        Args:
        dataset_location(`str`):
            The file path to the stored dataset. Currently only csv and pkl file types are supported

        Returns:
        `None`
        '''
        if (dataset_location.endswith('.csv')):  
            try:  
                loaded_dataset = Dataset.from_csv(
                    path_or_paths = dataset_location, 
                    encoding ='iso-8859-1',
                    keep_in_memory = True
                )
            except:
                for encoding in ENCODING_OPTIONS:
                    try:
                        temp_df = pd.read_csv(dataset_location, encoding = encoding)
                        loaded_dataset = Dataset.from_pandas(
                            temp_df
                        )
                        break
                    except:
                        continue
                else:
                    # The else clause is executed if the loop completes without encountering a break
                    raise Exception("Unable to read the file with any of the specified encodings")  

        elif (dataset_location.endswith('.pkl')):
            with open(dataset_location, "rb") as file:
                loaded_dataset = pickle.load(file)
        else:
            raise ValueError("Dataset creation for provided file type has not been defined")
    
        assert isinstance(loaded_dataset, Dataset)
        
        dataset_columns = list(loaded_dataset.features)
        
        extracted_with_cfg = all(col in dataset_columns for col in ['Source','Divider', 'Part', 'Tokens','Content'])
        if isinstance(loaded_dataset, Dataset) and extracted_with_cfg:
            
            if 'Modality' not in dataset_columns:
                loaded_dataset = loaded_dataset.add_column("Modality", ['text' for i in range(loaded_dataset.num_rows)])
            
            # to be safe, force all columns
            new_features = loaded_dataset.features.copy()
            new_features["Source"] = Value(dtype='string', id=None)
            new_features["Divider"] = Value(dtype='string', id=None)
            new_features["Part"] = Value(dtype='string', id=None)
            new_features["Tokens"] = Value(dtype='int64', id=None)
            new_features["Content"] = Value(dtype='string', id=None)
            
            try:
                loaded_dataset = loaded_dataset.cast(new_features, keep_in_memory=True)
            except AttributeError:
                # This catch is required due to a version change in the datasets package
                # Previously, there was no attribute called _batches which is required with the new `cast` method. This is missing from the pickle file
                # The solution is to reconstruct the dataset from a pandas frame
                loaded_dataset = Dataset.from_pandas(loaded_dataset.to_pandas())
                loaded_dataset = loaded_dataset.cast(new_features, keep_in_memory=True)
                
        elif isinstance(loaded_dataset, pd.DataFrame) and extracted_with_cfg:
            if 'Modality' not in dataset_columns:
                loaded_dataset["Modality"] = 'text'
            
            # to be safe, force all columns
            loaded_dataset["Source"] = loaded_dataset["Source"].astype(str)
            loaded_dataset["Divider"] = loaded_dataset["Divider"].astype(str)
            loaded_dataset["Part"] = loaded_dataset["Part"].astype(str)
            loaded_dataset["Tokens"] = loaded_dataset["Tokens"].astype(int)
            loaded_dataset["Content"] = loaded_dataset["Content"].astype(str)
        
        return loaded_dataset

    def save_dataset(
        self, 
        dataset_location: str
    ) -> None:
        '''
        Utility method to save datasets from object onto the disk. 

        Args:
        dataset_location(`str`):
            The file path to the write the dataset.

        Returns:
        `None`
        '''
        with open(dataset_location, "wb") as file:
            pickle.dump(self.ds, file)

    def load_encoded_vectors(
        self, 
        encoded_vectors_location: str
    ) -> None:
        '''
        Utility method to load stored embeddings from the disk.

        Args:
        encoded_vectors_location(`str`):
            The file path to the stored embeddings file. Currently only npy and pkl file types are supported

        Returns:
        `None`
        '''
        self.encoded_vectors = self._load_encoded_vectors(encoded_vectors_location = encoded_vectors_location)
        self.vector_dimensions = self.encoded_vectors.shape
    
        if self.metric_type_is_cosine_similarity:
            self.index = faiss.index_factory(self.vector_dimensions[1], "Flat", faiss.METRIC_INNER_PRODUCT)
        else:
            self.index = faiss.IndexFlatL2(self.vector_dimensions[1])

        self.index.add(self.encoded_vectors)

    def _load_encoded_vectors(
        self, 
        encoded_vectors_location: str
    ) -> np.ndarray:
        '''
        Internal method to load stored embeddings from the disk

        Args:
        encoded_vectors_location(`str`):
            The file path to the stored embeddings file. Currently only npy and pkl file types are supported

        Returns:
        `None`
        '''
        if (encoded_vectors_location.endswith('.npy')):
            encoded_vectors = np.load(encoded_vectors_location)
        else:
            with open(encoded_vectors_location, "rb") as file:
                encoded_vectors = pickle.load(file)

        assert isinstance(encoded_vectors, np.ndarray)
        
        return encoded_vectors

    def save_encoded_vectors(
        self, 
        encoded_vectors_location: str
    ) -> None :
        '''
        Utility method to save embeddings from object onto the disk. 

        Args:
        encoded_vectors_location(`str`):
            The file path to the write the dataset.

        Returns:
        `None`
        '''
        with open(encoded_vectors_location, "wb") as file:
            pickle.dump(self.encoded_vectors, file)

    def _concatenate_datasets(
        self,
        datasets: Union[List[Dataset], List[pd.DataFrame]],
    ) -> Union[Dataset, pd.DataFrame]:
        '''
        Interal utility method to concatenate datasets depending on the class type. Either pandas.DataFrame or datasets.Dataset

        Args:
        datasets(`Union[List[Dataset], List[pd.DataFrame]]`):
            A list of datasets where all the datasets of only of one type. Either pandas.DataFrame or datasets.Dataset 

        Returns:
        `Union[Dataset, pd.DataFrame]`
        '''
        return concatenate_datasets(datasets)

    def addDocumet(
        self, 
        documentFileLocation: List[str], 
        columns_to_index: Optional[List[str]], 
        columns_to_remove: Optional[List[str]] = [],
        target_column: Optional[str] = "text", 
        separator: Optional[str] = ',',
        keyword_search_params: Optional[Dict] = {},
        insight_id:Optional[str] = None,
    ) -> Dict:
        '''
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
        '''        
        # make sure they are all in indexed_files dir
        assert {os.path.basename(os.path.dirname(path)) for path in documentFileLocation} == {'indexed_files'}

        # create a list of the documents created so that we can push the files back to the cloud
        createDocumentsResponse = {
            'createdDocuments':[],
        }
    
        # loop through and embed new docs
        for document in documentFileLocation:
            # Get the directory path and the base filename without extension
            directory, base_filename = os.path.split(document)
            file_name_without_extension, file_extension = os.path.splitext(base_filename)
            new_file_extension = ".pkl"

            # Create the Dataset for every file
            dataset = self._load_dataset(dataset_location=document)

            if (columns_to_index == None or len(columns_to_index) == 0):
                columns_to_index = list(dataset.features)

            # save the dataset, this is for efficiency after removing docs
            new_file_path = os.path.join(
                directory, 
                file_name_without_extension + '_dataset' + new_file_extension
            )

            # if applicable, create the concatenated columns
            if (dataset.num_rows > 0):
                dataset = dataset.map(
                    self._concatenate_columns,           
                    fn_kwargs = {
                    "columns_to_index": columns_to_index, 
                    "target_column": target_column, 
                    "separator":separator
                    }
                )
                
                # transform chunks into keywords
                if keyword_search_params != None and keyword_search_params.pop('keywordSearch', None) is True:
                    keywords_for_target_col = self.keyword_engine.model(
                        input = dataset[target_column],
                        insight_id = insight_id,
                        param_dict = keyword_search_params
                    )[0]
                    #dataset = dataset.add_column(target_column, keywords_for_target_col)
                    dataset = dataset.remove_columns(column_names= target_column)
                    dataset = dataset.add_column(target_column, keywords_for_target_col)

                # get the embeddings for the document
                #vectors = self.embeddings_engine.get_embeddings(dataset[target_column])
                vectors = self.embeddings_engine.embeddings(
                    strings_to_embed = dataset[target_column], 
                    insight_id = insight_id
                )
                vectors = np.array(vectors[0]['response'], dtype=np.float32)
                assert vectors.ndim == 2

                columns_to_remove.append(target_column)
                columns_to_drop = list(set(columns_to_remove).intersection(set(dataset.features)))
                dataset = dataset.remove_columns(column_names= columns_to_drop)

                with open(new_file_path, "wb") as file:
                    pickle.dump(dataset, file)
                
                # add the created dataset file path
                createDocumentsResponse['createdDocuments'].append(new_file_path)
                
                # normalize the vectors if using huggingface
                if isinstance(self.tokenizer, HuggingfaceTokenizer):
                    faiss.normalize_L2(vectors)

                # write out the vectors with the same file name
                # Change the file extension to ".pkl"
                new_file_path = os.path.join(
                    directory, 
                    file_name_without_extension + '_vectors' + new_file_extension
                )
                with open(new_file_path, "wb") as file:
                    pickle.dump(vectors, file)

                # add the created embeddings file path
                createDocumentsResponse['createdDocuments'].append(new_file_path)

                # TODO need to update the flow for how we instatiate
                if (np.any(self.encoded_vectors) == None):
                    self.encoded_vectors = np.copy(vectors)
                    self.vector_dimensions = self.encoded_vectors.shape
                else:
                    # make sure the dimensions are the same
                    assert self.vector_dimensions[1] == vectors.shape[1]
                    self.encoded_vectors = np.concatenate([self.encoded_vectors, vectors], axis=0)

        master_indexClass_files, corrupted_file_sets = self.createMasterFiles(
            path_to_files=os.path.dirname(os.path.dirname(documentFileLocation[0]))
        )
        
        for corrupted_set in corrupted_file_sets:
            for file_path in corrupted_set:
                createDocumentsResponse['createdDocuments'].remove(file_path)
        
        createDocumentsResponse['createdDocuments'].extend(master_indexClass_files)

        return createDocumentsResponse

    def createMasterFiles(
        self, 
        path_to_files:str
    ) -> Tuple[str]:
        '''
        Create a master dataset and embeddings file based on the current documents. The main purpose of this is to improve startup runtime. 

        Args:
        path_to_files(`str`):
            The folder location of the indexed documents/datasets/embeddings

        Returns:
        `List[str]`
        '''
        created_documents, corrupted_docs, corrupted_file_sets = self._validateEmbeddingFiles(
            path_to_files=path_to_files,
        )
        
        return created_documents, corrupted_file_sets
    
    def _validateEmbeddingFiles(
        self,
        path_to_files: str,
        delete: bool = True
    ) -> Tuple:
        '''
        This method aims to validate the existing dataset and vector files and create new ones if necessary. It takes the path to the files and a boolean to determine if corrupted files should be deleted.

        The function operates by locating and loading all PDF files within a specified directory. After which it checks for corresponding CSV, dataset and vector files.

        In case dataset or vector files are found to be missing or corrupted, the method attempts to recreate them from the available CSV file, else it records the files under corrupted sets.

        Only documents with valid and verified dataset and vector files are stored for analysis or further usage.

        The function will additionally delete all identified corrupted files if delete is set to True. The final result is the list of created file paths, corrupted documents and the file data sets that were identified as corrupted.
        
        Args:
        path_to_files(`str`):
            The folder location of the index class/collection e.g. schema/default

        Returns `Tuple`: A tuple containing created_documents, corrupted_docs, corrupted_file_sets
            - created_documents is a `List[str]` containing the names master files names for files created during the validation process
            - corrupted_docs is a `Dict[str, str]` with the source document as the key and the read error of the dataset or vectors as the value
            - corrupted_file_sets is a `List[Tuple]` with the csv, dataset, vector and source files paths for corrupted sets
        '''
        documents_files_path = os.path.join(path_to_files, 'documents')
        indexed_files_path = os.path.join(path_to_files, 'indexed_files')

        # List all pdfs files in the directory
        source_documents = glob.glob(os.path.join(documents_files_path, "*"))

        valid_datasets_and_vectors = []
        corrupted_file_sets:List[Tuple] = []
        corrupted_docs:Dict[str, str] = {}
        created_documents:List[str] = []
        
        for full_source_path in source_documents:
            # get the basename of the file
            # all csvs, datasets and vectors should contain this base name
            pdf_file_name = os.path.basename(full_source_path)
            base_filename = os.path.splitext(pdf_file_name)[0]
            
            # get the file names for the dataset and vectors
            csv_file_name = base_filename + ".csv"
            dataset_file_name = base_filename + "_dataset.pkl"
            vector_file_name = base_filename + "_vectors.pkl"
            
            full_csv_path = os.path.join(indexed_files_path, csv_file_name)
            full_dataset_path = os.path.join(indexed_files_path, dataset_file_name)
            full_vector_path = os.path.join(indexed_files_path, vector_file_name)

            # if all the file paths exist, then create the tuple
            if os.path.exists(full_dataset_path) and os.path.exists(full_vector_path):
                # the next step is to validate non of these files are corrupted by attempting to load them all in
                
                try:
                    # try load the dataset
                    dataset = self._load_dataset(dataset_location=full_dataset_path)
                except Exception as e:
                    try:
                        # we can try save the dataset again from the csv
                        dataset = self._load_dataset(dataset_location=full_csv_path)
                        with open(full_dataset_path, "wb") as file:
                            pickle.dump(dataset, file)
                    except:
                        
                        corrupted_file_sets.append(
                            (full_csv_path, full_dataset_path, full_vector_path, full_source_path)
                        )
                        corrupted_docs[full_source_path] = "Couldn't load the csv file or save it as a dataset"
                        continue
                    
                    try:
                        # make sure we can load it in again
                        dataset = self._load_dataset(dataset_location=full_dataset_path)
                    except:
                        # we failed so record failure and continue on
                        corrupted_file_sets.append(
                            (full_csv_path, full_dataset_path, full_vector_path, full_source_path)
                        )
                        corrupted_docs[full_source_path] = "Couldn't load the dataset from the pickle file"
                        continue
                    
                try:
                    # try load the vectors
                    vectors = self._load_encoded_vectors(encoded_vectors_location=full_vector_path)
                except:
                    corrupted_file_sets.append(
                        (full_csv_path, full_dataset_path, full_vector_path, full_source_path)
                    )
                    corrupted_docs[full_source_path] = "Couldn't load the embeddings from the pickle file"
                    continue
                    
                # if we made it this far then all the files are not corrupted
                valid_datasets_and_vectors.append(
                    (dataset, vectors)
                )

        # bind the valid datasets and vectors
        if len(valid_datasets_and_vectors) > 0:
            self.ds = valid_datasets_and_vectors[0][0]
            self.encoded_vectors = valid_datasets_and_vectors[0][1]
            self.vector_dimensions = self.encoded_vectors.shape
            
            # loop through and concatenate the others if any
            for dataset, vectors in valid_datasets_and_vectors[1:]:
                self.ds = self._concatenate_datasets([self.ds, dataset])
                self.encoded_vectors = np.concatenate((self.encoded_vectors,vectors),axis=0)
                
            encoded_vectors_location = path_to_files + "/vectors.pkl"
            dataset_location = path_to_files + "/dataset.pkl"
            self.save_encoded_vectors(encoded_vectors_location = encoded_vectors_location)
            self.save_dataset(dataset_location = dataset_location)
            created_documents.append(encoded_vectors_location)
            created_documents.append(dataset_location)
            
            if (self.metric_type_is_cosine_similarity) and (self.vector_dimensions != None):
                self.index = faiss.index_factory(self.vector_dimensions[1], "Flat", faiss.METRIC_INNER_PRODUCT)
            elif (self.vector_dimensions != None):
                self.index = faiss.IndexFlatL2(self.vector_dimensions[1])

            if (self.encoded_vectors is not None):
                self.index.add(self.encoded_vectors)
            
        if delete:  
            for corrupted in corrupted_file_sets:
                for filename in corrupted:
                    try:
                        os.remove(filename)
                    except FileNotFoundError:
                        pass
                
        return created_documents, corrupted_docs, corrupted_file_sets
    
    def removeCorruptedFiles(
        self,
        path_to_files: str
    ) -> List[Tuple]:
        '''
        Check the vector index class/ collection for corrupted files and recreate the master files.
        
        Args:
        path_to_files(`str`):
            The folder location of the index class/collection e.g. schema/default
            
        Returns `List[Tuple]`: A list of tuples containing the csv, dataset, vector and source files paths for corrupted sets
        '''
        corrupted_files = self._validateEmbeddingFiles(
            path_to_files=path_to_files,
        )[1]
        
        return corrupted_files
    
    def datasetsLoaded(
        self
    ) -> bool:
        '''
        Check if data was loaded in from the csv
        
        Returns `bool`
        '''
        if (self.ds == None) or (list(self.ds.features) == []) or (len(list(self.ds.features)) == 0) or (self.ds.num_rows == 0):
            return False
        else:
            return True


    def do_rerank(self,
        question: str,
        distances: List,
        ann_index: List[int],
        result_count: int,
        columns_to_return: Optional[List[str]] = None,
        ascending : Optional[bool] = None
    ):
    # reranks based on an algorithm and then finds 
      
      
        if self.reranker_gaas_model is None:
            self.init_reranker()

      
        samples_df = pd.DataFrame(
            {
                'distances': distances, 
                'ann': ann_index
            }
        )
      
        #samples_df.sort_values(
        #    "distances", 
        #    ascending = (ascending if ascending is not None else self.default_sort_direction), 
        #    inplace=True
        #)
        #samples_df = samples_df[samples_df['distances'] <= return_threshold]
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
            output.update({'Score' : row['distances']})
            data_row = self.ds[int(row['ann'])]
            
            self.class_logger.info(f"Row to pick {int(row['ann'])}", extra={"stack": "BACKEND"})
            self.class_logger.info(f"[{str(data_row['Content'])}]", extra={"stack": "BACKEND"})
            
            for col in columns_to_return:
                #self.class_logger.warning(f"{col} {data_row[col]}", extra={"stack": "BACKEND"})
                output.update({col:data_row[col]})
                
            # this is not pythonic but let us try this for now
            #self.class_logger.warning(question, extra={"stack": "BACKEND"})
            try:
                if 'Content' in data_row.keys():
                    content = data_row['Content']
                else:
                    content = " ".join([str(val) for val in data_row.values()])
                    
                score = self.cross_encode(
                    [[question, content]]
                )
                
                output.update({'Sim': score})
            except:
                reranker_call_success = False
            
            final_output.append(output)

        # sort this by sim score
        if reranker_call_success:
            new_output = sorted(final_output, key=lambda x : x['Sim'], reverse=True)
        else:
            new_output = final_output
        
        # filter to the top x
        new_output = new_output[:result_count]

        return new_output
    
        # now comes the reranker 
    
    def cross_encode(self,
        pair: List[str]
    ):
        return self.reranker_gaas_model.model(input=pair)
    
    def init_reranker(self):
        self.reranker_gaas_model = ggm.ModelEngine(engine_id="30991037-1e73-49f5-99d3-f28210e6b95c12")
      
