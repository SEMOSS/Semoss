
from typing import Optional, Union, List, Dict, Any
from text_generation import Client
import requests
from .gaas_client_base import BaseClient
import inspect
from string import Template
from ..tokenizers import HuggingfaceTokenizer

class TextGenClient(BaseClient):
  params = list(inspect.signature(Client.generate).parameters.keys())[1:]

  def __init__(
      self, 
      template = None, 
      endpoint:str = None, 
      model_name:str = None,
      template_name:str = None, 
      stop_sequences:list = None, 
      timeout = 30,
      **kwargs
    ):
    assert endpoint is not None
    # assert model_name is not None
    super().__init__(
      template=template, 
      template_name = template_name
    )
    self.kwargs = kwargs
    self.client = Client(endpoint)
    self.client.timeout = timeout
    self.model_name = model_name
    self.model_list_endpoint=endpoint
    self.available_models = []
    self.template_name = template_name
    self.stop_sequences = stop_sequences
    # self.tokenizer = HuggingfaceTokenizer(
    #   encoder_name = model_name, 
    #   encoder_max_tokens = kwargs.pop(
    #     'max_tokens', 
    #     None
    #   )
    # )
    
  def ask(
      self, 
      question:str=None, 
      context:str=None,
      history:list=[],
      template_name:str=None,
      max_new_tokens:int=1000,
      prefix = "",
      **kwargs:Dict
    ) -> str:
    # start the prompt as an empty string
    prompt = ""
    
    if 'full_prompt' not in kwargs.keys():
      content = []

      # assert that the question is not None. We can do this or put an assert statement
      if question is None:
        return "Please ask me a question"
      
      if template_name == None:
        template_name = self.template_name

      if self.stop_sequences != None and 'kwargs' not in kwargs.keys():
        kwargs['stop_sequences'] = self.stop_sequences

      # attempt to pull in the context
      sub_occured = False
      mapping = {"question": question} | kwargs

      
      if context != None and template_name == None:
        # Case 1 Description: The user provided context string in the ask method. We need to check if this string might be a place holder
        # where the user wants to fill the context the provided as a template
        if isinstance(context, str):
          context, sub_occured = self.fill_context(context, **mapping)
          content = [context,'\n']
      elif context != None and template_name != None:
        # Case 2 Description: The user provided context string in the ask method. This context is intended to be string substituted 
        # into a template.
        # String substitution occurs if either 'question' or 'context' is substituted
        mapping.update({"context":context})
        context, sub_occured = self.fill_template(template_name=template_name, **mapping)
        content = [context,'\n']
      else:
        # Case 3 Description: There was no context provided, however the user might want to fill the question into a pre-defined template
        if template_name != None:
          possibleContent, sub_occured = self.fill_template(template_name=template_name, **mapping)
          if possibleContent != None:
            content = [possibleContent,'\n']

      ## Here we need to check if substitution occured, this determines how we might append history ##
      if sub_occured == False:
        # if substitution did not occur, they are only passing a context string like 'Your are a helpful Assistant'
        # Therefore, we want to append the history and and question/prompt directly after
        # Add history if one is provided
        for statement in history:
          content.append(statement['role'])
          content.append(':')
          content.append(statement['content'])
          content.append('\n')
      
        # append the user asked question to the content
        content.append('### Instruction:\n\n')
        content.append(question)
        content.append('\n')
        content.append('### Response:\n')

      else: 
        # Currently there is no template where only the context is substituted. At that point they should pass the context in as an argument.
        # Therefore, we assume that the question has been substitued and it needs to go last in the prompt string
        # As a result, we place the history first
        histContent = []
        for statement in history:
          histContent.append(statement['role'])
          histContent.append(':')
          histContent.append(statement['content'])
          histContent.append('\n')
        content = histContent + content

      # join all the inputs into a single string
      prompt = "".join(content)
      # ask the question and apply the additional params

      responses = self.client.generate_stream(prompt, max_new_tokens=max_new_tokens, **kwargs)
      final_response = ""
      for response in responses:
        chunk = response.token.text
        print(prefix+chunk,end='')
        final_response += chunk
      return final_response

    else:
      full_prompt = kwargs.pop('full_prompt')
      if isinstance(full_prompt, str):
        prompt = full_prompt
      elif isinstance(full_prompt, list):
        listOfDicts = set([isinstance(x, dict) for x in full_prompt]) == {True}
        if (listOfDicts == False):
            raise ValueError("The provided payload is not valid")
        # now we have to check the key value pairs are valid
        all_keys_set = {key for d in full_prompt for key in d.keys()}
        validOpenAiDictKey = sorted(all_keys_set) == ['content', 'role']
        if (validOpenAiDictKey == False):
          # this is because we are mimicing the OpenAI message payload structure
          raise ValueError("There are invalid dictionary keys")
        # add it the message payload
        for roleContent in full_prompt:
          role = roleContent['role'] if roleContent['role'].endswith('\n') else roleContent['role'] + ':\n\n'
          prompt += role
          message_content = roleContent['content'] if roleContent['content'].endswith('\n') else roleContent['content'] + '\n\n'
          prompt += message_content
      else:
          raise ValueError("Please either pass a string containing the full prompt or a sorted list that contains dictionaries with only 'role' and 'content' keys.\nPlease note, the values associated with 'role' and 'content' should contain the appropriate character to build a prompt string.S")

      print(prompt)
      responses = self.client.generate_stream(prompt, max_new_tokens=max_new_tokens, **kwargs)
      final_response = ""
      for response in responses:
        chunk = response.token.text
        print(prefix+chunk,end='')
        final_response += chunk
      return final_response
