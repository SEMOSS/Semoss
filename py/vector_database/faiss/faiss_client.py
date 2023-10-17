from typing import List, Dict, Union, Optional
import transformers
from datasets import Dataset, concatenate_datasets, load_dataset, disable_caching
import pandas as pd
import faiss
import numpy as np
from ..encoders.huggingface_encoder import HuggingFaceEncoder
import pickle
import os
import glob

class FAISSSearcher():
  '''
  The primary class for a faiss database classes and searching document embeddings
  '''

  def __init__(self, df=None, 
               ds=None, 
               tokenizer_model=None, 
               model_model=None, 
               tokenizer_loader=transformers.AutoTokenizer, 
               model_loader=transformers.AutoModel, 
               dpr=False,
               encoder_class=None,
               base_path = None,
               ):
    # if df is None and ds is None:
    #  return "Both dataframe and dataset cannot be none"
    if(tokenizer_model is not None):
      self.tokenizer = tokenizer_loader.from_pretrained(tokenizer_model)
    if(model_model is not None):
      self.model = model_loader.from_pretrained(model_model)
      if not dpr:
        self.model.to(self.device)
    self.init_device()
    if df is not None:
      self.ds = Dataset.from_pandas(df)
      if '__index_level_0__' in self.ds.column_names:
        self.ds = self.ds.remove_columns('__index_level_0__')
    if ds is not None:
      self.ds = ds
    self.dpr = dpr
    self.encoded_vectors = None
    self.vector_dimensions = None
    self.encoder_name = None

    assert encoder_class is not None
    self.encoder_class = encoder_class
    self.base_path = base_path

    # disable caching within the shell so that engines can be exported
    disable_caching()
   
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

    self.ds = concatenate_datasets([self.ds, appendDs])
    conc_vector = np.concatenate((self.encoded_vectors,new_vector),axis=0)
    vector_dimension = conc_vector.shape[1]
    self.index = faiss.IndexFlatL2(vector_dimension)
    self.encoded_vectors = np.copy(conc_vector)
    faiss.normalize_L2(conc_vector)
    self.index.add(conc_vector)
    
  def nearestNeighbor(
      self, 
      question: str, 
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
    search_vector = self.encoder_class.get_embeddings(question)
    _vector = np.array([search_vector])

    # check to see if need to normalize the vector
    if isinstance(self.encoder_class, HuggingFaceEncoder):
      faiss.normalize_L2(_vector)

    # perform the faiss search. Scores returned are Euclidean distances
    # euclidean_distances - the measurement score between the embedded question and the Approximate Nearest Neighbor (ANN)
    # ann_index - the index location of the Approximate Nearest Neighbor (ANN)
    euclidean_distances, ann_index = self.index.search(_vector, k = results)

    # create a data
    samples_df = pd.DataFrame({'distances': euclidean_distances[0], 'ann': ann_index[0]})
    samples_df.sort_values("distances", ascending=ascending, inplace=True)
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
    ) -> None:
    '''
    Internal method to load the dataset based on its file type

    Args:
      dataset_location(`str`):
        The file path to the stored dataset. Currently only csv and pkl file types are supported

    Returns:
     `None`
    '''
    if (dataset_location.endswith('.csv')):
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
    
    assert isinstance(loaded_dataset, Dataset)
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
    ) -> None:
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

  def addDocumet(
      self, 
      documentFileLocation: List[str], 
      columns_to_index: List[str], 
      columns_to_remove: List[str] = [],
      target_column: str = "text", 
      separator: str = ','
    ) -> None:
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

      # get the embeddings for the document
      vectors = self.encoder_class.get_embeddings(dataset[target_column])
      assert vectors.ndim == 2

      columns_to_remove.append(target_column)
      columns_to_drop = list(set(columns_to_remove).intersection(set(dataset.features)))
      dataset = dataset.remove_columns(column_names= columns_to_drop)

      with open(new_file_path, "wb") as file:
        pickle.dump(dataset, file)

      # write out the vectors with the same file name
      # Change the file extension to ".pkl"
      new_file_path = os.path.join(directory, file_name_without_extension + '_vectors' + new_file_extension)
      with open(new_file_path, "wb") as file:
        pickle.dump(vectors, file)

      # TODO need to update the flow for how we instatiate
      if (np.any(self.encoded_vectors) == None):
        self.encoded_vectors = np.copy(vectors)
        self.vector_dimensions = self.encoded_vectors.shape
      else:
        # make sure the dimensions are the same
        assert self.vector_dimensions[1] == vectors.shape[1]
        self.encoded_vectors = np.concatenate([self.encoded_vectors, vectors], axis=0)

    self.createMasterFiles(path_to_files=os.path.dirname(documentFileLocation[0]))

  def createMasterFiles(
      self, 
      path_to_files:str
    ) -> None :
    '''
    Create a master dataset and embeddings file based on the current documents. The main purpose of this is to improve startup runtime. 

    Args:
      path_to_files(`str`):
        The folder location of the indexed documents/datasets/embeddings

    Returns:
      `None`
    '''
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
        self.ds = concatenate_datasets([self.ds, dataset])

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
    self.save_encoded_vectors(encoded_vectors_location=baseFolder + "/vectors.pkl")
    self.save_dataset(dataset_location=baseFolder + "/dataset.pkl")

    self.index = faiss.IndexFlatL2(self.vector_dimensions[1])
    if isinstance(self.encoder_class, HuggingFaceEncoder):
      faiss.normalize_L2(self.encoded_vectors)    

    self.index.add(self.encoded_vectors)
