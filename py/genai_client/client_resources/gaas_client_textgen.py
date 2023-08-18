
from text_generation import Client
import requests
from genai_client.client_resources.gaas_client_base import BaseClient
import inspect
from string import Template

class TextGenClient(BaseClient):
  params = list(inspect.signature(Client.generate).parameters.keys())[1:]

  def __init__(self, template_file=None, endpoint=None, model_name="guanaco", **kwargs):
    assert endpoint is not None
    super().__init__(template_file=template_file)
    self.kwargs = kwargs
    self.client = Client(endpoint)
    self.model_name = model_name
    self.model_list_endpoint=endpoint
    self.available_models = []
    
  def ask(self, 
          question:str=None, 
          context:str=None,
          history:list=[],
          template_name:str=None,
          **kwargs:dict
          )->str:
    # start the prompt as an empty string
    prompt = ""
    
    # TODO try to clean this up
    content = []

    # assert that the question is not None. We can do this or put an assert statement
    if question is None:
      return "Please ask me a question"

    # attempt to pull in the context
    sub_occured = False
    mapping = {"question": question} | kwargs
    if context != None:
       # check if we have to fill the content/template passed in by the user
       context, sub_occured = self.fill_context(context, **mapping)
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
      content.append('user:')
      content.append(question)
      content.append('\n')
      content.append('system:')

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
    
    # ask the question and apply the additional params
    response = self.client.generate(prompt, **kwargs)
    return response.generated_text.strip()

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
