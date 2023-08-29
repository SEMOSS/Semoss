
from text_generation import Client
import requests
from genai_client.client_resources.gaas_client_base import BaseClient
import inspect
from string import Template

class TextGenClient(BaseClient):
  params = list(inspect.signature(Client.generate).parameters.keys())[1:]

  def __init__(self, template=None, endpoint=None, model_name="guanaco", template_name = None, **kwargs):
    assert endpoint is not None
    super().__init__(template=template)
    self.kwargs = kwargs
    self.client = Client(endpoint)
    self.model_name = model_name
    self.model_list_endpoint=endpoint
    self.available_models = []
    self.template_name = template_name
    
  def ask(self, 
          question:str=None, 
          context:str=None,
          history:list=[],
          template_name:str=None,
          max_new_tokens:int=100,
          prefix = "",
          **kwargs:dict
          )->str:
    # start the prompt as an empty string
    prompt = ""
    
    # TODO try to clean this up
    content = []

    # assert that the question is not None. We can do this or put an assert statement
    if question is None:
      return "Please ask me a question"
    
    if template_name == None:
       template_name = self.template_name

    # attempt to pull in the context
    sub_occured = False
    mapping = {"question": question} | kwargs
    if context != None and template_name == None:
      # check if we have to fill the content/template passed in by the user
      if isinstance(context, str):
        context, sub_occured = self.fill_context(context, **mapping)
        content = [context,'\n']
    elif context != None and template_name != None:
        mapping.update({"context":context})
        context, sub_occured = self.fill_template(template_name=template_name, **mapping)
        content = [context,'\n']
    else:
      if template_name != None:
        possibleContent, sub_occured = self.fill_template(template_name=template_name, **mapping)
        if possibleContent != None:
           content = [possibleContent,'\n']

    # if substitution did not occure, they are only passing a context string like 'Your are a helpful Assistant'
    if sub_occured == False:
      # Add history if one is provided
      for statement in history:
        content.append(statement['role'])
        content.append(':')
        content.append(statement['content'])
        content.append('\n')
    
      # append the user asked question to the content
      content.append('System:\n')
      content.append(question)
      content.append('\n')
      content.append('User:\n')

    else: # oterhwise the gave how the want the response to come back, so we reverse order - history first
      histContent = []
      for statement in history:
        histContent.append(statement['role'])
        histContent.append(':')
        histContent.append(statement['content'])
        histContent.append('\n')
      content = histContent + content

    # join all the inputs into a single string
    prompt = "".join(content)
    #print(prompt)
    # ask the question and apply the additional params
    responses = self.client.generate_stream(prompt, max_new_tokens=max_new_tokens, **kwargs)
    final_response = ""
    for response in responses:
       chunk = response.token.text
       print(prefix+chunk,end='')
       final_response += chunk
    return final_response

  def get_available_models(self)->list:
    if len(self.available_models) == 0:
        availableModelRequest = requests.get(self.model_list_endpoint)
        if (availableModelRequest.status_code == 200):
            if availableModelRequest.content != b'':
                self.available_models = [modelInfo['id'] for modelInfo in availableModelRequest.json()['data']]
            else:
                if (not self.model_list_endpoint.endswith('/')):
                    self.model_list_endpoint = self.model_list_endpoint + '/'
                self.available_models = [self.model_list_endpoint.split('/')[-2]]
    
    return self.available_models
  
  def is_model_available(self,model_name:str)->bool:
    return (model_name in self.get_available_models())
