import pandas as pd

from typing import Any, Dict, List, Mapping, Optional, Union
import gaas_gpt_model as ggm
from gaas_gpt_model import ModelEngine
from sentence_transformers import SentenceTransformer
import numpy as np
import os
import pickle
import glob
import json

try:
    import simplejson as json
except ImportError:
    import json  # type: ignore

import uuid
from datetime import date, datetime
from decimal import Decimal
INTEGER_TYPES = ()
FLOAT_TYPES = (Decimal,)
TIME_TYPES = (date, datetime)

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

# CFG/SEMOSS packages
from genai_client import HuggingfaceTokenizer
from genai_client import get_tokenizer
import gaas_gpt_model as ggm
from ..constants import ENCODING_OPTIONS

Hosts = Union[str, List[Union[str, Mapping[str, Union[str, int]]]]]

class OpenSearchConnection():
    '''
    This is the primary class to connect to an Open Search Database  
    '''

    def __init__(
            self, 
            index_name: str,
            tokenizer,
            distance_method: str,
            embedder_engine_id: str,
            indexList: list = [str],
            embedder_engine: ModelEngine = None,
           **kwargs,
    )-> None:
        '''
        Create an instance of a OpenSearchConnetor 
        TODO Want to double check if we should have certs on
        '''
        # first we have to determine what tokenizer we need
        self.tokenizer = tokenizer


        if embedder_engine is not None:
            self.embeddings_engine = embedder_engine
        else:
            self.embeddings_engine = ModelEngine(engine_id = embedder_engine_id) 

        self.encoded_vectors = None
        self.vector_dimensions = None



    # def getClient(self)-> OpenSearch:
    #     '''
    #     Return the client
    #     '''
    #     # return self.client
    #     return None
    
    def count_documents(self) -> int: 
        '''
        Returns the number of documents in the index
        '''
        # return self.getClient().count(index=self.index_name)["count"]
        return 0    

    # Createing a copy of the add document, this will return a map to the java side to make a rest call
    def addDocumetAndCreateMapping(
        self, 
        documentFileLocation: List[str], 
        columns_to_index: Optional[List[str]], 
        columns_to_remove: Optional[List[str]] = [],
        target_column: Optional[str] = "text", 
        separator: Optional[str] = ',',
        keyword_search_params: Optional[Dict] = {},
        insight_id:Optional[str] = None,
    ) -> Dict:
        # make sure they are all in indexed_files dir
        assert {os.path.basename(os.path.dirname(path)) for path in documentFileLocation} == {'indexed_files'}

         # create a list of the documents created so that we can push the files back to the cloud
        createDocumentsResponse = {
            'createdDocuments':[],}
        
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
                
                # # transform chunks into keywords NOTSURE IF I SHOULD WORRY ABOUT KEYWORDS JUST YET 
                # if keyword_search_params != None and keyword_search_params.pop('keywordSearch', None) is True:
                #     keywords_for_target_col = self.keyword_engine.model(
                #         input = dataset[target_column],
                #         insight_id = insight_id,
                #         param_dict = keyword_search_params
                #     )[0]
                #     #dataset = dataset.add_column(target_column, keywords_for_target_col)
                #     dataset = dataset.remove_columns(column_names= target_column)
                #     dataset = dataset.add_column(target_column, keywords_for_target_col)

                # get the embeddings for the document
                #vectors = self.embeddings_engine.get_embeddings(dataset[target_column])
                vectors = self.embeddings_engine.embeddings(
                    strings_to_embed = dataset[target_column], 
                    insight_id = insight_id
                )
                vectors = np.array(vectors[0]['response'], dtype=np.float32)
                assert vectors.ndim == 2

                
                # self.getClient().index(index = self.index_name, body = vectors, id = indexCountStr, refresh=True)
                body: Dict[str, Any] = {
                    "my_vector1": vectors[0]
                }

                columns_to_remove.append(target_column)
                columns_to_drop = list(set(columns_to_remove).intersection(set(dataset.features)))
                dataset = dataset.remove_columns(column_names= columns_to_drop)

                with open(new_file_path, "wb") as file:
                    pickle.dump(dataset, file)
                
                # add the created dataset file path
                createDocumentsResponse['createdDocuments'].append(new_file_path)

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


        # TODO NEED TO COME BACK AND FIX MASTERFILE SET UP AND SAVING ... 
        # TODO CREATE A UTILITY TO DO THIS
        # master_indexClass_files, corrupted_file_sets = self.createMasterFiles(
        #     path_to_files=os.path.dirname(os.path.dirname(documentFileLocation[0]))
        # )
        
        # for corrupted_set in corrupted_file_sets:
        #     for file_path in corrupted_set:
        #         createDocumentsResponse['createdDocuments'].remove(file_path)
        
        # createDocumentsResponse['createdDocuments'].extend(master_indexClass_files)

        return json.dumps(body, default=self.default, ensure_ascii=False, separators=(",", ":"))

   

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

   
    
    def _createDictFromReturn(self, hit: Dict[str, Any]) -> Dict:
        """
        Helper method to create a Dict from the search hit provided.
        """
        score = hit["_score"]
        sourceData = hit["_source"]
        sourceData.pop('embedding', None)
        output = {}
        output.update({'Score' :score})
        output.update({'Source': sourceData})
        return output
    
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
    

    def createBodyForKNN(
            self, 
            question:str,
            insight_id: str,
            limit:Optional[int] = 3,
    ):
        if not question:
            msg = "query must be a non empty string"
            raise ValueError(msg)
        
        search_vector = self.embeddings_engine.embeddings(
            strings_to_embed = [question], 
            insight_id = insight_id
         )
        query_vector = np.array(search_vector[0]['response'], dtype=np.float32)
        query_vector = query_vector[0]
        #query_vector = query_vector.tolist()
        # query_vector = np.array2string(query_vector, threshold = np.inf)
        body: Dict[str, Any] = {
                "size": limit,
                "query": {
                    "knn": {
                        "my_vector1": {
                        "vector": query_vector,
                        "k": limit
                        }
                    }
                }
            }
        return json.dumps(
                body, default=self.default, ensure_ascii=False, separators=(",", ":")
            )
    
    def default(self, data: Any) -> Any:
        if isinstance(data, TIME_TYPES):
            # Little hack to avoid importing pandas but to not
            # return 'NaT' string for pd.NaT as that's not a valid
            # date.
            formatted_data = data.isoformat()
            if formatted_data != "NaT":
                return formatted_data

        if isinstance(data, uuid.UUID):
            return str(data)
        elif isinstance(data, FLOAT_TYPES):
            return float(data)
        elif INTEGER_TYPES and isinstance(data, INTEGER_TYPES):
            return int(data)

        # Special cases for numpy and pandas types
        # These are expensive to import so we try them last.
        try:
            import numpy as np

            if isinstance(
                data,
                (
                    np.int_,
                    np.intc,
                    np.int8,
                    np.int16,
                    np.int32,
                    np.int64,
                    np.uint8,
                    np.uint16,
                    np.uint32,
                    np.uint64,
                ),
            ):
                return int(data)
            elif isinstance(
                data,
                (
                    np.float_,
                    np.float16,
                    np.float32,
                    np.float64,
                ),
            ):
                return float(data)
            elif isinstance(data, np.bool_):
                return bool(data)
            elif isinstance(data, np.datetime64):
                return data.item().isoformat()
            elif isinstance(data, np.ndarray):
                return data.tolist()
        except ImportError:
            pass

        try:
            import pandas as pd

            if isinstance(data, (pd.Series, pd.Categorical)):
                return data.tolist()
            elif isinstance(data, pd.Timestamp) and data is not getattr(
                pd, "NaT", None
            ):
                return data.isoformat()
            elif data is getattr(pd, "NA", None):
                return None
        except ImportError:
            pass

        raise TypeError("Unable to serialize %r (type: %s)" % (data, type(data)))

