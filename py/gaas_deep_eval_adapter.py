from ai_server import ServerClient

from deepeval.models import DeepEvalBaseLLM
from pydantic import BaseModel
from ai_server import ModelEngine

from lmformatenforcer import JsonSchemaParser
from lmformatenforcer.integrations.transformers import (
    build_transformers_prefix_allowed_tokens_fn,
)

import json


from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline


class SEMOSSDeepEvalAdapter(DeepEvalBaseLLM):
  def __init__(self, secret_key=None, access_key=None, base_url=None, model_id=None, insight_id=None, model_name=None, **params):
    if access_key is not None and secret_key is not None:
      self.loginKeys = {"secretKey":secret_key,"accessKey":access_key}
      self.server_connection = ServerClient(access_key=self.loginKeys['accessKey'],
      secret_key=self.loginKeys['secretKey'],
      base=base_url)
    self.model_id = model_id
    if insight_id is not None:
      self.insight_id = insight_id
    else:
      self.insight_id = self.server_connection.cur_insight
    self.hyper_params = params
    self.model_name = model_name
    if self.model_name is not None:
      self.model_name = self.model_id
    self.model_name = "meta-llama/CodeLlama-13b-hf"
    self.model = AutoModelForCausalLM.from_pretrained(self.model_name, load_in_8bit=True, local_files_only=True, device_map="cuda")
    self.tokenizer = AutoTokenizer.from_pretrained(self.model_name, device=self.model.device, use_fast=False)
    self.pipe = pipeline(
            "text-generation",
            model=self.model,
            tokenizer=self.tokenizer,
            use_cache=True,
            device_map="auto",
            max_length=2500,
            do_sample=True,
            top_k=5,
            num_return_sequences=1,
            eos_token_id=self.tokenizer.eos_token_id,
            pad_token_id=self.tokenizer.eos_token_id,
        )    
    

  def load_model(self):
    #self.model = ModelEngine(engine_id=self.model_id, insight_id=self.insight_id)
    return self.model

  def generate(self, prompt: str, schema: BaseModel) -> BaseModel:

    model = self.load_model()
    self.prompt = prompt
    self.schema = schema
    print(schema)
    #prompt_output = model.ask(prompt) #,self.hyper_params)
    #prompt_response = prompt_output[0]['response']
    
    #[{'generated_text': 'hello = {\n  name = "hello"\n  type = "web"\n '}]
    #output = [{'generated_text': prompt_output[0]['response']}]
    
    parser = JsonSchemaParser(schema.schema())
    prefix_function = build_transformers_prefix_allowed_tokens_fn(self.pipe.tokenizer, parser)
    self.prompt_response = self.pipe(prompt, prefix_allowed_tokens_fn=prefix_function)
    
    print(self.prompt_response)
    output = self.prompt_response[0]['generated_text'][len(prompt) :]
    str_output = json.dumps(output)
    self.json_output = json.loads(str_output)
    self.json_output = json.loads(self.json_output)
    return schema(**self.json_output)
      
  async def a_generate(self, prompt: str, schema: BaseModel) -> BaseModel:
    return self.generate(prompt, schema)

  def get_model_name(self):
    return self.model_name