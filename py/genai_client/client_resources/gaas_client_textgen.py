
from text_generation import Client
import requests
from genai_client.client_resources.gaas_client_base import BaseClient
import inspect

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
          context:list=[], 
          template_name:str=None, 
          stop_sequences=["#", ";"],
          **kwargs:dict
          )->str:
    # start the prompt as an empty string
    prompt = ""
    
    # default the template name based on model name
    if template_name != None and self.model_name in self.templates:
      content = [self.templates[template_name],'\n']
    elif f"{self.model_name}.default.nocontext" in self.templates:
      content = [self.templates[f"{self.model_name}.default.nocontext"],'\n']
    
    # Add history if one is provided
    for history in context:
        content.append(history['role'])
        content.append(':')
        content.append(history['content'])
        content.append('\n')
    
    # assert that the question is not None. We can do this or put an assert statement
    if question is None:
      return "Please ask me a question"

    # append the user asked question to the content
    content.append('user:')
    content.append(question)

    # join all the inputs into a single string
    prompt = "".join(content)

    print(prompt)
    
    # ask the question and apply the additional params
    response = self.client.generate(prompt, stop_sequences=stop_sequences, **kwargs)
    return response.generated_text

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
