from typing import Optional, Union, List, Dict, Any
import openai
from .gaas_client_base import BaseClient
from ..tokenizers import OpenAiTokenizer
import json
from string import Template
import tiktoken
import numpy as np

class OpenAiClient(BaseClient):
  def __init__(
    self, 
    endpoint:str = None, 
    model_name:str = None, 
    api_key:str = "EMPTY", 
    **kwargs
  ):
    super().__init__(
      template = kwargs.pop(
        'template', 
        None
      ),
      template_name = kwargs.pop(
        'template_name', 
        None
      )
    )

    self.model_name = model_name
    
    self.api_key = api_key
    openai.api_key = self.api_key
    if (endpoint != None):
      self.endpoint = endpoint
      openai.api_base = endpoint

    self.chat_type = 'chat-completion'
    if 'chat_type' in kwargs.keys():
      self.chat_type = kwargs.pop('chat_type')

    self.tokenizer = OpenAiTokenizer(
      encoder_name = model_name, 
      encoder_max_tokens = kwargs.pop(
        'max_tokens', 
        None
      )
    )

    self.kwargs = kwargs
    
  def ask(
    self, 
    question:str = None, 
    context:str = None, 
    template_name:str = None, 
    history:List[Dict] = None, 
    page_size = 100, 
    prefix="", 
    **kwargs
  ) -> str:

    # TODO - make this better or pass the correct keys to the FE
    if ('max_new_tokens' in kwargs.keys()):
      kwargs['max_tokens'] = int(kwargs.pop('max_new_tokens'))
    if ('repetition_penalty' in kwargs.keys()):
      kwargs['frequency_penalty'] = float(kwargs.pop('repetition_penalty'))
    if ('stop_sequences' in kwargs.keys()):
      kwargs['stop'] = kwargs.pop('stop_sequences')

    if template_name == None:
       template_name = self.template_name

    # first we determine the type of completion, since this determines how we
    # structure the payload
    final_query = ""
    if self.chat_type == 'chat-completion':
      # the list to construct the payload from
      message_payload = []

      if 'full_prompt' not in kwargs.keys():
        # if the user provided context, use that. Otherwise, try to get it from the template
        message_payload = self._process_chat_completion(
          question = question,
          context = context,
          history = history,
          template_name = template_name,
          fill_variables = kwargs
        )

      else:
        message_payload = self._process_full_prompt(
          kwargs.pop('full_prompt')
        )

      num_input_tokens = self.tokenizer.count_tokens(input = message_payload)
      self._check_input_token_limits(num_input_tokens)

      # add the message payload as a kwarg
      kwargs['messages'] = message_payload
      
      responses = openai.ChatCompletion.create(model=self.model_name, stream = True, **kwargs)
      num_generated_tokens = 0
      for chunk in responses:
        response = chunk.choices[0].get('delta', {}).get('content')
        if response != None:
          num_generated_tokens += self.tokenizer.count_tokens(input = response)
          final_query += response
          print(prefix+response, end ='')

      final_query = final_query

    elif self.chat_type == 'completion':
      prompt = ""
      mapping = {"question": question}

      # default the template name based on model
      if template_name is None and context is None:
        template_name = f"{self.model_name}.default.nocontext"
      elif context is not None:
        template_name = f"{self.model_name}.default.context" 

      # generate the prompt
      if context is not None:
        mapping = {"question": question, "context":context}
      # merge kwargs
      mapping = mapping | kwargs

      prompt = super().fill_template(template_name=template_name, **mapping)

      if prompt is None:
        prompt = question

      # Add history if one is provided
      if history is not None:
        prompt = f"{prompt} {history}"

      final_query = question + " "
      tokens = 0
      finish = False

      print(prefix+final_query, end ='')
      responses = openai.Completion.create(model=self.model_name, prompt=question, stream = True, **kwargs)
      for chunk in responses:
        response = chunk.choices[0].text
        if response != None:
           final_query += response
           print(prefix+response, end ='')
      final_query = final_query
    return final_query

  def _process_chat_completion(
    self, 
    question:str,
    context:str,
    history:List[Dict],
    template_name:str,
    fill_variables:Dict
  ) -> List[Dict]:
    # the list to construct the payload from
    message_payload = []
    
    # if the user provided context, use that. Otherwise, try to get it from the template
    mapping = {"question": question} | fill_variables
    if context is not None and template_name == None:
      if isinstance(context, str):
        context = self.fill_context(context, **mapping)[0]
        message_payload.append({"role": "system", "content": context})
    elif context != None and template_name != None:
      mapping.update({"context":context})
      context = self.fill_template(template_name=template_name, **mapping)[0]
      message_payload.append({"role": "system", "content": context})
    else:
      if template_name != None:
        possibleContent = self.fill_template(template_name=template_name, **mapping)[0]
        if possibleContent != None:
          message_payload.append({"role": "system", "content": possibleContent})

    # if history was added, then add it to the payload. Currently history is being like OpenAI prompts
    if history is not None:
      message_payload.extend(history)

    # add the new question to the payload
    if (question != None and len(question) > 0):
      message_payload.append({"role": "user", "content": question})

    return message_payload

  def _process_completion(self):
    pass

  def _process_full_prompt(self, full_prompt: List) -> List[Dict]:
    if isinstance(full_prompt, list):
      listOfDicts = set([isinstance(x, dict) for x in full_prompt]) == {True}
      if (listOfDicts == False):
        raise ValueError("The provided payload is not valid")
      # now we have to check the key value pairs are valid
      all_keys_set = {key for d in full_prompt for key in d.keys()}
      validOpenAiDictKey = sorted(all_keys_set) == ['content', 'role']
      if (validOpenAiDictKey == False):
        raise ValueError("There are invalid OpenAI dictionary keys")
      # add it the message payload
      return full_prompt
    else:
      raise TypeError("Please make sure the full prompt for OpenAI Chat-Completion is a list")

  def _check_input_token_limits(self, input_tokens:int):
    if (self.tokenizer.get_max_token_length() != None):
      if (input_tokens >= self.tokenizer.get_max_token_length()):
        raise ValueError('The message you submitted was too long, please reload the conversation and submit something shorter.')

  def embeddings(self, list_to_encode:List[str], embedding_model:str = None, prefix:str = "") -> List[float]:
    # need to make sure the embedding model was passed
    if (embedding_model == None):
        if (self.model_name == None):
            raise ValueError("Please specify an embedding model in order to utilize this method")
        else:
            embedding_model = self.model_name

    # Make sure a list was passed in so we can proceed with the logic below
    assert isinstance(list_to_encode, list)
    
    total_num_of_tokens = self.tokenizer.count_tokens(''.join(list_to_encode))

    embedded_list = [] # This is the final list that will be sent back
    if (self.max_tokens != None):
      if (total_num_of_tokens <= self.max_tokens):
        # The entire list can be sent as a single batch
        print(prefix + "Waiting for OpenAI to process all chunks")
        single_batch_results = self._make_openai_embedding_call(
          list_to_encode, 
          embedding_model=embedding_model
        )

        embedded_list = [vector['embedding'] for vector in single_batch_results['data']]

      else:
        # Split the list into batches
        current_batch = []
        current_token_count = 0
        batches = []

        for chunk in list_to_encode:
            chunk_token_count = self.tokenizer.count_tokens(chunk)

            if current_token_count + chunk_token_count <= self.max_tokens:
                current_batch.append(chunk)
                current_token_count += chunk_token_count
            else:
                # Start a new batch
                batches.append(current_batch)
                current_batch = [chunk]
                current_token_count = chunk_token_count

        if len(current_batch) > 0:
          batches.append(current_batch)

        print(prefix + "Multiple batches have to be sent to OpenAI")
        number_of_batches = len(batches)
        for i in range(number_of_batches):
          batch_results = self._make_openai_embedding_call(
            batches[i], 
            embedding_model=embedding_model
          )
          print(prefix + "Completed Embedding " + str(i+1) + "/" + str(number_of_batches) + " Batches")
          embedded_list.extend([vector['embedding'] for vector in batch_results['data']])
        
    else:
      # We have no choice but to try send the entire thing
      print(prefix + "Waiting for OpenAI to process all chunks")
      single_batch_results = self._make_openai_embedding_call(
        list_to_encode, 
        embedding_model=embedding_model
      )

      embedded_list = [vector['embedding'] for vector in single_batch_results['data']]
    
    print(prefix + "Sending Embeddings back from Model Engine")
    return embedded_list

  @staticmethod
  def _make_openai_embedding_call(text:Any, embedding_model:str="text-embedding-ada-002"):
      '''this method is responsible for making the openai embeddings call. it takes in a single'''
      return openai.Embedding.create(input = text, model=embedding_model)