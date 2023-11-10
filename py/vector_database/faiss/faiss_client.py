from typing import List, Dict, Union, Optional, Any
import transformers
from datasets import Dataset, concatenate_datasets, load_dataset, disable_caching
import pandas as pd
import faiss
import numpy as np
from ..encoders import *
import pickle
import os
import glob

class FAISSSearcher():
  '''
  The primary class for a faiss database classes and searching document embeddings
  '''

  datasetType = 'datasets'

  def __init__(
      self, 
      encoder_class,
      tokenizer,
      base_path = None,
    ):
    # if df is None and ds is None:
    #  return "Both dataframe and dataset cannot be none"

    self.init_device()
    self.ds = None

    self.encoded_vectors = None
    self.vector_dimensions = None

    self.encoder_class = encoder_class
    self.tokenizer = tokenizer

    self.base_path = base_path

    # disable caching within the shell so that engines can be exported
    disable_caching()

  def __getattr__(self, name: str):
      return self.__dict__[f"_{name}"]
  
  def __setattr__(self, name:str, value:Any):
      '''
      Enfore types for specific attributes
      '''
      if name == 'encoded_vectors' or value != None:
        if name in ['ds']:
            if not isinstance(value, (pd.DataFrame, Dataset)):
                raise TypeError(f"{name} must be a pd.DataFrame or Dataset")
        elif name in ['encoder_class']:
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
      row, 
      columns_to_index=None, 
      target_column=None, 
      separator="\n"
    ) -> Dict:
    text = ""
    for col in columns_to_index:
      text += str(row[col])
      text += separator
    return {target_column : text}
    
  def init_device(self):
    '''
    Utility method to determine whether or not the devie running the interpreter has a gpu
    '''
    import torch
    if torch.cuda.is_available():       
      self.device = torch.device("cuda")
      #print("Using GPU.")
    else:
      #print("No GPU available, using the CPU instead.")
      self.device = torch.device("cpu")
  
  def appendToIndex(self, dataObj= None, target_column="text",columns_to_index=None , separator="\n"):
    if(columns_to_index is None):
      columns_to_index = list(self.ds.features)
  
    #previous_encoded_vector = np.load(filePath)
    appendDs = Dataset.from_dict({})
    if (isinstance(dataObj, pd.DataFrame)):
      appendDs = Dataset.from_pandas(dataObj)
      if '__index_level_0__' in appendDs.column_names:
        appendDs = appendDs.remove_columns('__index_level_0__')
    elif (isinstance(dataObj, Dataset)):
      appendDs = dataObj
    elif (isinstance(dataObj, dict)):
      appendDs = Dataset.from_dict(dataObj)
    else:
      raise ValueError("Undefined class check: dataObj is of an unrecognized type")
    
    appendDs = appendDs.map(self._concatenate_columns, fn_kwargs={"columns_to_index": columns_to_index, "target_column": target_column, "separator":separator})

    new_vector = self.encoder_class.get_embeddings(appendDs[target_column])

    assert self.encoded_vectors.shape[1] == new_vector.shape[1]

    self.ds = self._concatenate_datasets([self.ds, appendDs])
    conc_vector = np.concatenate((self.encoded_vectors,new_vector),axis=0)
    vector_dimension = conc_vector.shape[1]
    self.index = faiss.IndexFlatL2(vector_dimension)
    self.encoded_vectors = np.copy(conc_vector)
    faiss.normalize_L2(conc_vector)
    self.index.add(conc_vector)
    
  def nearestNeighbor(
      self, 
      question: str,
      insight_id:str,
      filter: Optional[str] = None,
      results: Optional[int] = 5, 
      columns_to_return: Optional[List[str]] = None, 
      return_threshold: Optional[Union[int,float]] = 1000, 
      ascending : Optional[bool] = True
    ) -> List[Dict]:
    '''
    Find the closest match(es) between the question bassed in and the embedded documents using Euclidena Distance.

    Args:
      question(`str`):
        The string you are trying to match against the embedded documents
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

    # make sure the encoder class is loaded and get the embeddings (vector) for the tokens
    # search_vector = self.encoder_class.get_embeddings([question])
    # query_vector = np.array([search_vector])

    search_vector = self.encoder_class.embeddings(
        strings_to_embed = [question], 
        insight_id = insight_id
    )
    query_vector = np.array(search_vector[0])
    assert query_vector.shape[0] == 1

    # check to see if need to normalize the vector
    if isinstance(self.encoder_class, HuggingFaceEncoder):
      faiss.normalize_L2(query_vector)

    # perform the faiss search. Scores returned are Euclidean distances
    # euclidean_distances - the measurement score between the embedded question and the Approximate Nearest Neighbor (ANN)
    # ann_index - the index location of the Approximate Nearest Neighbor (ANN)

    # If a filter was passed in then we need to get the indexes
    if filter != None:
        filter_ids = self._filter_dataset(filter)
        id_selector = faiss.IDSelectorArray(filter_ids)
        euclidean_distances, ann_index = self.index.search(
            query_vector, 
            k = results, 
            params=faiss.SearchParametersIVF(sel=id_selector)
        )
    else:
        euclidean_distances, ann_index = self.index.search( 
            query_vector, 
            k = results
        )

    euclidean_distances = euclidean_distances[0]
    ann_index = ann_index[0]

    # this is a safety check to make sure we are only returning good vectors if the limit was too high
    if self.vector_dimensions[0] < results:
        # Find the index of the first occurrence of -1
        index_of_minus_one = np.where(ann_index == -1)[0]
        # If -1 is not found, index_of_minus_one will be an empty array
        # In that case, we keep the original array, otherwise, we slice it
        if len(index_of_minus_one) > 0:
            ann_index = ann_index[:index_of_minus_one[0]]
            euclidean_distances = euclidean_distances[:index_of_minus_one[0]]

    # create the data
    samples_df = pd.DataFrame(
        {
            'distances': euclidean_distances, 
            'ann': ann_index
        }
    )
    samples_df.sort_values(
        "distances", 
        ascending = ascending, 
        inplace=True
    )
    samples_df = samples_df[samples_df['distances'] <= return_threshold]
    
    # create the response payload by adding the relevant columns from the dataset
    final_output = []
    for _, row in samples_df.iterrows():
      output = {}
      output.update({'Score' : row['distances']})
      data_row = self.ds[int(row['ann'])]
      for col in columns_to_return:
        output.update({col:data_row[col]})
      final_output.append(output)
      
    return final_output
  
  def _filter_dataset(self, filter:str) -> List[int]:
    if isinstance(self.ds, Dataset):
      filterDf = self.ds.to_pandas()
    else:
      filterDf = self.ds

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
    Internal method to load the dataset based on its file type

    Args:
      dataset_location(`str`):
        The file path to the stored dataset. Currently only csv and pkl file types are supported

    Returns:
     `None`
    '''
    if (dataset_location.endswith('.csv')):
      if (FAISSSearcher.datasetType == 'pandas'):
        loaded_dataset = pd.read_csv(filepath_or_buffer=dataset_location, encoding='iso-8859-1')
      else:
        try:
          loaded_dataset = Dataset.from_csv(
            dataset_location, 
            encoding='iso-8859-1',
            keep_in_memory=True
          )
        except:
          loaded_dataset = load_dataset(
            'csv', 
            data_files = dataset_location,
            keep_in_memory=True
          )
    elif (dataset_location.endswith('.pkl')):
      with open(dataset_location, "rb") as file:
        loaded_dataset = pickle.load(file)
    else:
      raise ValueError("Dataset creation for provided file type has not been defined")
    
    assert isinstance(loaded_dataset, (Dataset, pd.DataFrame)) 
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
    self.index = faiss.IndexFlatL2(self.vector_dimensions[1])
    if isinstance(self.encoder_class, HuggingFaceEncoder):
      faiss.normalize_L2(self.encoded_vectors)   
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
    if (FAISSSearcher.datasetType == 'pandas'):
      return pd.concat(datasets, axis=1, verify_integrity=True)
    else:
      return concatenate_datasets(datasets)

  def addDocumet(
      self, 
      documentFileLocation: List[str], 
      insight_id:str,
      columns_to_index: Optional[List[str]], 
      columns_to_remove: Optional[List[str]] = [],
      target_column: Optional[str] = "text", 
      separator: Optional[str] = ',',
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

    Returns:
      `None`
    '''
    # make sure they are all in indexed_files dir
    assert {os.path.basename(os.path.dirname(path)) for path in documentFileLocation} == {'indexed_files'}

    # create a list of the documents created so that we can push the files back to the cloud
    createDocumentsResponse = {
      'createdDocuments':[],
      'documentsWithLargerChunks': {}
    }
    
    # loop through and embed new docs
    for document in documentFileLocation:
      # Get the directory path and the base filename without extension
      directory, base_filename = os.path.split(document)
      file_name_without_extension, file_extension = os.path.splitext(base_filename)
      new_file_extension = ".pkl"

      # Create the Dataset for every file
      # TODO change this to json so we dont have encoding issue
      dataset = Dataset.from_csv(
        path_or_paths = document, 
        encoding ='iso-8859-1',
        keep_in_memory = True
      )

      if (columns_to_index == None or len(columns_to_index) == 0):
        columns_to_index = list(dataset.features)

      # save the dataset, this is for efficiency after removing docs
      new_file_path = os.path.join(
        directory, 
        file_name_without_extension + '_dataset' + new_file_extension
      )

      # if applicable, create the concatenated columns
      dataset = dataset.map(
        self._concatenate_columns,           
        fn_kwargs = {
          "columns_to_index": columns_to_index, 
          "target_column": target_column, 
          "separator":separator
        }
      )

      # need to check that the chunks are not greater than what the tokenizer can handle
      chunks_with_larger_tokens = self._check_chunks_token_size(dataset[target_column])
      createDocumentsResponse['documentsWithLargerChunks'][document] = chunks_with_larger_tokens

      # get the embeddings for the document
      #vectors = self.encoder_class.get_embeddings(dataset[target_column])
      vectors = self.encoder_class.embeddings(
        strings_to_embed = dataset[target_column], 
        insight_id = insight_id
      )
      vectors = np.array(vectors[0])
      assert vectors.ndim == 2

      columns_to_remove.append(target_column)
      columns_to_drop = list(set(columns_to_remove).intersection(set(dataset.features)))
      dataset = dataset.remove_columns(column_names= columns_to_drop)

      with open(new_file_path, "wb") as file:
        pickle.dump(dataset, file)
      
      # add the created dataset file path
      createDocumentsResponse['createdDocuments'].append(new_file_path)

      # write out the vectors with the same file name
      # Change the file extension to ".pkl"
      new_file_path = os.path.join(directory, file_name_without_extension + '_vectors' + new_file_extension)
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

    master_indexClass_files = self.createMasterFiles(path_to_files=os.path.dirname(documentFileLocation[0]))
    createDocumentsResponse['createdDocuments'].extend(master_indexClass_files)

    return createDocumentsResponse

  def createMasterFiles(
      self, 
      path_to_files:str
    ) -> List[str]:
    '''
    Create a master dataset and embeddings file based on the current documents. The main purpose of this is to improve startup runtime. 

    Args:
      path_to_files(`str`):
        The folder location of the indexed documents/datasets/embeddings

    Returns:
      `None`
    '''
    # create a list of the documents created so that we can push the files back to the cloud
    created_documents = []

    # Define the pattern for the files you want to find
    file_pattern = '*_dataset.pkl'

    # Use glob.glob to find all matching files in the directory
    matching_files = glob.glob(os.path.join(path_to_files, file_pattern))
    for i, file in enumerate(matching_files):
      print(file)
      dataset = self._load_dataset(dataset_location=file)
      if i == 0:
        self.ds = dataset
      else:
        self.ds = self._concatenate_datasets([self.ds, dataset])

    # Define the pattern for the files you want to find
    file_pattern = '*_vectors.pkl'

    # Use glob.glob to find all matching files in the directory
    matching_files = glob.glob(os.path.join(path_to_files, file_pattern))
    for i, file in enumerate(matching_files):
      print(file)
      vectors = self._load_encoded_vectors(encoded_vectors_location=file)
      if i == 0:
        self.encoded_vectors = vectors
      else:
        self.encoded_vectors = np.concatenate((self.encoded_vectors,vectors),axis=0)

    # TODO create master files - maybe ? Need to do performance comparision
    baseFolder = path_to_files=os.path.dirname(path_to_files)
    encoded_vectors_location = baseFolder + "/vectors.pkl"
    dataset_location = baseFolder + "/dataset.pkl"
    self.save_encoded_vectors(encoded_vectors_location = encoded_vectors_location)
    self.save_dataset(dataset_location = dataset_location)
    created_documents.append(encoded_vectors_location)
    created_documents.append(dataset_location)

    self.index = faiss.IndexFlatL2(self.vector_dimensions[1])
    if isinstance(self.encoder_class, HuggingFaceEncoder):
      faiss.normalize_L2(self.encoded_vectors)    

    self.index.add(self.encoded_vectors)

    return created_documents

  def _check_chunks_token_size(self, strings_to_embed:List[str]):
    max_token_length = self.tokenizer.get_max_token_length()
    number_of_chunks = len(strings_to_embed)
    chunks_with_higher_tokens = []
    for i in range(number_of_chunks):
      chunk =  strings_to_embed[i]
      tokens_in_chunk = self.tokenizer.count_tokens(chunk)
      if (tokens_in_chunk > max_token_length):
        chunks_with_higher_tokens.append(i)
    
    return chunks_with_higher_tokens
