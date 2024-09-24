import guidance
import json
from transformers import AutoModelForCausalLM, AutoTokenizer

# model_name = "meta-llama/CodeLlama-13b-hf"

class JSONMaker:

  def init(self, model_name=None, tokenizer_name=None, file_name=None, snapshot=None, repo_type=None, local_files_only=True, context=None, **kwargs):
    # try to see the repo type
    # load it accordingly
    # This code will assume Cuda
    # this also assumes the files are available locally i.e. it is not pulling the files
    # expects the files to be in the huggingface hub
    
    self.model = AutoModelForCausalLM.from_pretrained(model_name,local_files_only=local_files_only, device_map="cuda", **kwargs)
    if tokenizer_name is None:
      tokenizer_name = model_name
      
    self.tokenizer = AutoTokenizer.from_pretrained(model_name, device=self.model.device, use_fast=False)
    self.code_llm = guidance.models.Transformers(model=self.model, tokenizer=self.tokenizer, echo=False)
  
  def toJSON(self,json_schema=None, text_data=None, context=None):
    # convert prompt with context
    this_context = "You are an expert who converts english to json. When data is not available, use -111 as default value. Convert the following sentences to json.  "
    if context is not None:
      this_context = context
    
    output = {}

    
    if text_data is None:
      output.update({"input": "no data provided"})
      return output
    else:
      output.update({"input": text_data})
      
    if json_schema is None:
      output.update({"schema": "no schema provided"})
      return output
    else:
      output.update({"schema" : json_schema})
      json_schema = json.loads(json_schema)
    
    # generate the value
    final_data = this_context + text_data
    json_output = self.code_llm + final_data
    json_output += guidance.json(name="generated_object", schema=json_schema)
    
    json_output = json_output['generated_object']
    
    if json_output is not None:
      output.update({"output": json_output})
    else:
      output.update({"output": "failed to generate output"})
      
    return output
    
    
    
    