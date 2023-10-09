from typing import List
import transformers
from datasets import Dataset, concatenate_datasets, load_dataset
import pandas as pd
import faiss
import numpy as np
from ..encoders.huggingface_encoder import HuggingFaceEncoder
import pickle
import os
import glob

class FAISSSearcher():
  '''this the primary class for a faiss database table (if that notion makes sense)'''

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
    self.faiss_encoder_loaded = False
    self.encoded_vectors = None
    self.vector_dimensions = None
    self.encoder_name = None
    self.encoder_class = encoder_class
    self.base_path = base_path
   
  
  def concatenate_columns(self, row, columns_to_index=None, target_column=None, separator="\n"):
    text = ""
    #print(row)
    #print(columns_to_index)
    for col in columns_to_index:
      #print(row)
      text += str(row[col])
      text += separator
    return {target_column : text}
    
  def init_device(self):
    import torch
    if torch.cuda.is_available():       
      self.device = torch.device("cuda")
      #print("Using GPU.")
    else:
      #print("No GPU available, using the CPU instead.")
      self.device = torch.device("cpu")
      
  
  ###############################################################################
  # FAISS
  ###############################################################################
  
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
    
    appendDs = appendDs.map(self.concatenate_columns, fn_kwargs={"columns_to_index": columns_to_index, "target_column": target_column, "separator":separator})

    self.load_faiss_encoder()
    new_vector = self.faiss_encoder.get_embeddings(appendDs[target_column])

    assert self.encoded_vectors.shape[1] == new_vector.shape[1]

    self.ds = concatenate_datasets([self.ds, appendDs])
    conc_vector = np.concatenate((self.encoded_vectors,new_vector),axis=0)
    vector_dimension = conc_vector.shape[1]
    self.index = faiss.IndexFlatL2(vector_dimension)
    self.encoded_vectors = np.copy(conc_vector)
    faiss.normalize_L2(conc_vector)
    self.index.add(conc_vector)
  
  def load_faiss_encoder(self, encoder_name="paraphrase-mpnet-base-v2"):
    if not self.faiss_encoder_loaded:
      if self.encoder_class != None:
        self.faiss_encoder = self.encoder_class 
      else:
        self.faiss_encoder = HuggingFaceEncoder(embedding_model = encoder_name, api_key = "EMPTY")

      self.faiss_encoder_loaded = True
      self.encoder_name = encoder_name
    
  def nearestNeighbor(self, 
                       question, 
                       results=5, 
                       columns_to_return: List[str] = None, 
                       json=True, 
                       print_result=False,
                       return_threshold = 1000, 
                       ascending = False
                       ):
    if(columns_to_return is None):
      columns_to_return = list(self.ds.features)
    self.load_faiss_encoder()
    search_vector = self.faiss_encoder.get_embeddings(question)
    _vector = np.array([search_vector])

    if isinstance(self.encoder_class, HuggingFaceEncoder):
      faiss.normalize_L2(_vector)  
    distances, ann = self.index.search(_vector, k=results)
    #print("results.. ")
    samples_df = pd.DataFrame({'distances': distances[0], 'ann': ann[0]})
    samples_df.sort_values("distances", ascending=ascending, inplace=True)
    samples_df = samples_df[samples_df['distances'] <= return_threshold]
    #print(samples_df)
    
    final_output = []
    docs = []
    for _, row in samples_df.iterrows():
      output = {}
      output.update({'Score' : row['distances']})
      #print("-"*30)
      #print(row['ann'])
      data_row = self.ds[int(row['ann'])]
      #print(f"Score : {row['distances']}")
      for col in columns_to_return:
        if(print_result):
          print(f"{col} : {data_row[col]}")
        output.update({col:data_row[col]})
        docs.append(f"{col}:{data_row[col]}")
      final_output.append(output)
      
    if json:
      return final_output
    else:
      return docs
  
  def load_index(self, index_location):
    self.load_faiss_encoder()
    self.index = faiss.read_index(index_location)

  def save_index(self, index_location):
    faiss.write_index(self.index, index_location)

  def load_dataset(self, dataset_location:str):
    self.ds = self._load_dataset(dataset_location = dataset_location)

  def _load_dataset(self, dataset_location:str):
    if (dataset_location.endswith('.csv')):
      try:
        loaded_dataset = load_dataset('csv', data_files= dataset_location)
      except:
        loaded_dataset = Dataset.from_csv(dataset_location, encoding='iso-8859-1')
    elif (dataset_location.endswith('.pkl')):
      with open(dataset_location, "rb") as file:
        loaded_dataset = pickle.load(file)
    else:
      raise ValueError("Dataset creation for provided file type has not been defined")
    
    assert isinstance(loaded_dataset, Dataset)
    return loaded_dataset

  def save_dataset(self, dataset_location):
    with open(dataset_location, "wb") as file:
      pickle.dump(self.ds, file)

  def load_encoded_vectors(self, encoded_vectors_location:str):
    self.encoded_vectors = self._load_encoded_vectors(encoded_vectors_location = encoded_vectors_location)
    self.vector_dimensions = self.encoded_vectors.shape
    self.index = faiss.IndexFlatL2(self.vector_dimensions[1])
    if isinstance(self.encoder_class, HuggingFaceEncoder):
      faiss.normalize_L2(self.encoded_vectors)   
    self.index.add(self.encoded_vectors)

  def _load_encoded_vectors(self, encoded_vectors_location:str):
    if (encoded_vectors_location.endswith('.npy')):
      encoded_vectors = np.load(encoded_vectors_location)
    else:
      with open(encoded_vectors_location, "rb") as file:
        encoded_vectors = pickle.load(file)

    assert isinstance(encoded_vectors, np.ndarray)
    return encoded_vectors

  def save_encoded_vectors(self, encoded_vectors_location):
    with open(encoded_vectors_location, "wb") as file:
      pickle.dump(self.encoded_vectors, file)
    #np.save(encoded_vectors_location, self.encoded_vectors)

  # TODO need to create a util function that writes out all the files (index, Dataset and vectors) based on a csv
  # it should also register it within the obj at the same time
  def addDocumet(self, 
                 documentFileLocation: List[str], 
                 columns_to_index: List[str], 
                 columns_to_remove: List[str] = [],
                 target_column: str = "text", 
                 separator: str = ','
                 ) -> None:
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
      dataset = Dataset.from_csv(document, encoding='iso-8859-1')
      # if file_extension == '.csv':
      #   try:
      #     dataset = load_dataset('csv', data_files= document)
      #   except:
      #     dataset = Dataset.from_csv(document, encoding='iso-8859-1')

      if (columns_to_index == None or len(columns_to_index) == 0):
        columns_to_index = list(dataset.features)

      # save the dataset, this is for efficiency after removing docs
      new_file_path = os.path.join(
        directory, 
        file_name_without_extension + '_dataset' + new_file_extension
      )

      # if applicable, create the concatenated columns
      dataset = dataset.map(
        self.concatenate_columns,           
        fn_kwargs = {
          "columns_to_index": columns_to_index, 
          "target_column": target_column, 
          "separator":separator
        }
      )

      # TODO need to change how this works
      self.load_faiss_encoder()

      # get the embeddings for the document
      vectors = self.faiss_encoder.get_embeddings(dataset[target_column])
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

  def createMasterFiles(self, path_to_files:str):
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
