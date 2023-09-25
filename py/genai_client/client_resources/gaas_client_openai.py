
import openai
from genai_client.client_resources.gaas_client_base import BaseClient
import json
from string import Template

class OpenAiClient(BaseClient):
  # template to use
  # prefix
  #'vicuna-7b-v1.3'
  # openai.api_base = "https://play.semoss.org/fastchat/v1/"
  def __init__(self, template=None, endpoint=None, model_name='gpt-3.5-turbo', api_key="EMPTY", template_name = None, **kwargs):
    super().__init__(template=template)
    self.kwargs = kwargs
    self.model_name = model_name
    self.instance_history = []
    if (endpoint!=None):
      self.endpoint = endpoint
    else:
      self.endpoint = openai.api_base
    assert self.endpoint is not None
    self.api_key = api_key
    self.available_models = []
    self.chat_type = 'chat-completion'
    if 'chat_type' in kwargs.keys():
      self.chat_type = kwargs.pop('chat_type')
    self.template_name = template_name
    
  def ask(self, 
          question:str=None, 
          context:str=None, 
          template_name:str=None, 
          history:list=None, 
          page_size=100, 
          prefix="", 
          **kwargs):
    openai_base = openai.api_base
    # forcing the api_key to a dummy value
    if openai.api_key is None:
      openai.api_key = self.api_key
    openai.api_base = self.endpoint

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
        mapping = {"question": question} | kwargs
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
        
        # add the message payload as a kwarg
        kwargs['messages'] = message_payload
        
        responses = openai.ChatCompletion.create(model=self.model_name, stream = True, **kwargs)
        for chunk in responses:
          response = chunk.choices[0].get('delta', {}).get('content')
          if response != None:
            final_query += response
            print(prefix+response, end ='')
        final_query = final_query
      else:
        full_prompt = kwargs.pop('full_prompt')
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
          message_payload.extend(full_prompt)
        
        kwargs['messages'] = message_payload
        completion = openai.ChatCompletion.create(model=self.model_name, **kwargs)
        response = completion.choices[0].message.content
        # TODO - need a way to define return type
        # print(type(response))
        # try:
        #   json_output = json.loads(response)
        #   print('OpenAI response:',json_output)
        #   return json_output
        # except:
        #   pass
        final_query = response

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

      ## TODO - remove this. Tempory solution until fastchat endpoints are up again
      # if ('max_tokens' in kwargs.keys()):
      #   max_new_tokens = kwargs.pop('max_tokens')
      # else:
      #   max_new_tokens = 100

      #print('Prompt is:',prompt)
      #print('Q is:',question)
      print(prefix+final_query, end ='')
      responses = openai.Completion.create(model=self.model_name, prompt=question, stream = True, **kwargs)
      for chunk in responses:
        response = chunk.choices[0].text
        #print('Printing reponses')
        if response != None:
           final_query += response
           print(prefix+response, end ='')
      final_query = final_query
      # while tokens < max_new_tokens and not finish:
      #   full_prompt = f"{prompt}{final_query}"
      #   response = openai.Completion.create(model=self.model_name, prompt=full_prompt, max_tokens=page_size, **kwargs)
      #   output=response.choices[0].text
      #   print(prefix+output)
      #   final_query = f"{final_query}{output}"
      #   tokens = tokens + page_size
      #   finish = response.choices[0].finish_reason == "stop"
    openai.api_base = openai_base  
    return final_query

  def embeddings(self, question:str = None)->dict:
    openai_base = openai.api_base
    # forcing the api_key to a dummy value
    if openai.api_key is None:
      openai.api_key = self.api_key
    openai.api_base = self.endpoint
    return openai.Embedding.create(model=self.model_name, input=question)
  
  def get_available_models(self)->list:
    if len(self.available_models) == 0:
      self.available_models = [model.get('root') for model in openai.Model.list()['data']]
    return self.available_models
  
  def is_model_available(self,model_name:str)->bool:
    return (model_name in self.get_available_models())