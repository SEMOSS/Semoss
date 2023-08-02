import openai
import requests
from genai_client.client_resources.gaas_client_openai import OpenAiClient

class FastChatClient(OpenAiClient):
  
  def __init__(self, model_name = 'vicuna-7b-v1.3', endpoint = None, **kwargs):
    # if ('template_file' in kwargs.keys()):
    #     super().__init__(template_file=kwargs['template_file'])
    if (endpoint==None):
      kwargs['endpoint'] = "https://play.semoss.org/fastchat/v1"
    kwargs['model_name'] = model_name
    super().__init__(**kwargs)

    # this will change once we know where the actual model repi will be hosted
    self.model_list_endpoint="https://play.semoss.org/fastchat/v1/models"
    if ('model_list_endpoint' in kwargs.keys()):
      self.model_list_endpoint = kwargs['model_list_endpoint']

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