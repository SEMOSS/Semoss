import transformers
from datasets import Dataset, concatenate_datasets, load_dataset
import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModel, AutoModelForSeq2SeqLM, AutoModelForQuestionAnswering, pipeline
import numpy as np
import smssutil
from ..encoders.huggingface_encoder import HuggingFaceEncoder
import pickle

# https://raw.githubusercontent.com/yashprakash13/datasets/master/arxiv_short.csv
#from transformers import DPRContextEncoder, DPRContextEncoderTokenizer
#ctx_encoder = DPRContextEncoder.from_pretrained("facebook/dpr-ctx_encoder-single-nq-base")
#ctx_tokenizer = DPRContextEncoderTokenizer.from_pretrained("facebook/dpr-ctx_encoder-single-nq-base")

# initialize for DPRContextEncoder
#  f2 = fa.FAISSSearcher(df=df, tokenizer_model="facebook/dpr-ctx_encoder-single-nq-base", model_model="facebook/dpr-ctx_encoder-single-nq-base", tokenizer_loader=transformers.DPRContextEncoderTokenizer, model_loader=transformers.DPRContextEncoder, dpr=True)

# initialize for generic
#  f = fa.FAISSSearcher(df=df)


# encoder models
# - sentence-transformers/all-mpnet-base-v2 - Decent - 384 tokens
# multi-qa-mpnet-base-dot-v1 - Trained on 215M not so great.. would not use - also 512 tokens
# - paraphrase-mpnet-base-v2 - 512 tokens
# sentence-transformers/facebook-dpr-ctx_encoder-single-nq-base - 509 tokens
# all-distilroberta-v1 - 512 tokens trained on 1B - Similar to others - Kind of similar to DPR
#"sentence-transformers/multi-qa-mpnet-base-dot-v1"

# text pattern = '\\s\\[1-9]+\\.\\d+[-]?\\d+'

class FAISSSearcher():

  def __init__(self, df=None, 
               ds=None, 
               tokenizer_model=None, 
               model_model=None, 
               tokenizer_loader=transformers.AutoTokenizer, 
               model_loader=transformers.AutoModel, 
               dpr=False,
               encoder_class=None,
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
    self.lfqa_loaded = False
    self.qa_loaded = False
    self.summarizer_loaded = False
    self.encoded_vectors = None
    self.vector_dimensions = None
    self.encoder_name = None
    self.encoder_class = encoder_class
   
  
  def concatenate_columns(self, row, columns_to_index=None, target_column=None, separator="\n"):
    text = ""
    #print(row)
    #print(columns_to_index)
    for col in columns_to_index:
      #print(row)
      text += str(row[col])
      text += separator
    return {target_column : text}  
    
  def cls_pooling(self, model_output):
    return model_output.last_hidden_state[:, 0]

  def get_embeddings(self, text_list):
    #lambda example: {'embeddings': ctx_encoder(**ctx_tokenizer(example["line"], return_tensors="pt"))[0][0].numpy()}
    encoded_input = self.tokenizer(
        text_list, padding=True, truncation=True, return_tensors="pt"
    )
    encoded_input = {k: v.to(self.device) for k, v in encoded_input.items()}
    model_output = self.model(**encoded_input)
    return self.cls_pooling(model_output)
    
  def get_embeddings_dpr(self, text_list):
    encoded_input = self.tokenizer(
        text_list, padding=True, truncation=True, return_tensors="pt"
    )
    #encoded_input = {k: v for k, v in encoded_input.items()}
    model_output = self.model(**encoded_input)[0][0].numpy()
    return model_output
    
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
  
  # the other paraphraser - "paraphrase-mpnet-base-v2"
  # sentence-transformers/facebook-dpr-ctx_encoder-single-nq-base
  def custom_faiss_index(self, columns_to_index=None, target_column="text", embedding_column="embeddings", encoder_name="paraphrase-mpnet-base-v2", separator="\n"):
    #take the columns and concatenate them together
    if(columns_to_index is None):
      columns_to_index = list(self.ds.features)
    # concatenate columns
    #print(self.ds)
    self.ds = self.ds.map(self.concatenate_columns, fn_kwargs={"columns_to_index": columns_to_index, "target_column": target_column, "separator":separator})
    #get the column to index
    self.load_faiss_encoder()
    vectors = self.faiss_encoder.get_embeddings(self.ds[target_column])
    self.encoded_vectors = np.copy(vectors)
    vector_dimension = vectors.shape[1]
    self.index = faiss.IndexFlatL2(vector_dimension)
    faiss.normalize_L2(vectors)
    self.index.add(vectors)
  
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
    
  def get_result_faiss(self, question, results=5, target_columns=None, json=True, print_result=False, index=None, ds=None):
    if ds is None:
      ds = self.ds
    if(target_columns is None):
      target_columns = list(ds.features)
    self.load_faiss_encoder()
    search_vector = self.faiss_encoder.get_embeddings(question)
    _vector = np.array([search_vector])
    faiss.normalize_L2(_vector)   
    if index is None:
      index = self.index
    distances, ann = index.search(_vector, k=results)
    #print("results.. ")
    samples_df = pd.DataFrame({'distances': distances[0], 'ann': ann[0]})
    samples_df.sort_values("distances", ascending=False, inplace=True)
    #print(samples_df)
    
    final_output = []
    docs = []
    for _, row in samples_df.iterrows():
      output = {}
      output.update({'Score' : row['distances']})
      #print("-"*30)
      #print(row['ann'])
      data_row = ds[int(row['ann'])]
      #print(f"Score : {row['distances']}")
      for col in target_columns:
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
    if (dataset_location.endswith('.csv')):
      loaded_dataset = load_dataset('csv', data_files= dataset_location)
    elif (dataset_location.endswith('.pkl')):
      with open(dataset_location, "rb") as file:
        loaded_dataset = pickle.load(file)
    else:
      raise ValueError("Dataset creation for provided file type has not been defined")
    
    assert isinstance(loaded_dataset, Dataset)
    self.ds = loaded_dataset

  def save_dataset(self, dataset_location):
    with open(dataset_location, "wb") as file:
      pickle.dump(self.ds, file)

  def load_encoded_vectors(self, encoded_vectors_location:str):
    if (encoded_vectors_location.endswith('.npy')):
      encoded_vectors = np.load(encoded_vectors_location)
    else:
      with open(encoded_vectors_location, "rb") as file:
        encoded_vectors = pickle.load(file)

    assert isinstance(encoded_vectors, np.ndarray)
    self.encoded_vectors = encoded_vectors

  def save_encoded_vectors(self, encoded_vectors_location):
    with open(encoded_vectors_location, "wb") as file:
      pickle.dump(self.encoded_vectors, file)
    #np.save(encoded_vectors_location, self.encoded_vectors)

  # TODO need to create a util function that writes out all the files (index, Dataset and vectors) based on a csv
  # it should also register it within the obj at the same time
  def addDocumet(self, target_column:str, columns_to_index:list, documentFileLocation:str, separator:str = ',') -> None:
    for document in documentFileLocation:
      
      # Create the Dataset for every file
      # TODO change this to json so we dont have encoding issue
      dataset = load_dataset('csv', data_files= document)

      # if applicable, create the concatenated columns
      dataset = dataset.map(self.concatenate_columns, 
                            fn_kwargs={
                              "columns_to_index": columns_to_index, 
                              "target_column": target_column, 
                              "separator":separator
                            }
                )

       # TODO need to change how this works
      self.load_faiss_encoder()


      vectors = self.faiss_encoder.get_embeddings(dataset[target_column])
      assert vectors.ndim == 2

      # TODO need to update the flow for how we instatiate
      if (self.encoded_vectors == None):
        self.encoded_vectors = np.copy(vectors)
        self.vector_dimensions = self.encoded_vectors.shape
      else:
        # make sure the dimensions are the same
        assert self.vector_dimensions[1] == vectors.shape[1]
        self.encoded_vectors = np.concatenate([self.encoded_vectors, vectors], axis=0)

      self.index = faiss.IndexFlatL2(self.vector_dimensions[1])
      faiss.normalize_L2(vectors)
      self.index.add(vectors)