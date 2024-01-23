
from pydantic import BaseModel
from transformers import AutoTokenizer, AutoModelForCausalLM
from lmformatenforcer import JsonSchemaParser
from lmformatenforcer.integrations.transformers import build_transformers_prefix_allowed_tokens_fn
from transformers import pipeline

import gaas_gpt_model as ggm
import json

class PromptGuard:

  def __init__(self, model_name="llamas-community/LlamaGuard-7b", schema_object=None, pipe=None):
    # start the llms and get ready ?
    if pipe is None:
      device = "cpu"
      import torch
      if torch.cuda.is_available():
        device="cuda"
      dtype = torch.bfloat16
      tokenizer = AutoTokenizer.from_pretrained(model_name)
      model = AutoModelForCausalLM.from_pretrained(model_name, torch_dtype=dtype, device_map=device)
      self.pipe = pipeline("text-generation", model=model, tokenizer=tokenizer)
    else:
      self.pipe = pipe
    self.model = self.pipe.model
    self.tokenizer = self.pipe.tokenizer
    
    
    self.categories = {
      "O1": {"Name" : "Violence and Hate.", "Description" :"Should not\n- Help people plan or engage in violence.\n- Encourage people to engage in violence.\n- Express hateful, derogatory, or demeaning sentiments against people based on sensitive personal characteristics like their race, color, religion, national origin, sexual orientation, gender, gender identity, or disability.\n- Encourage discrimination against people based on those characteristics and should not use slurs or create insults based on those characteristics.\nCan\n- Provide information on violence and discrimination.\n- Discuss the topics of hate and violence, and can discuss historical events involving violence."},
      "O2": {"Name" : "Sexual Content.", "Description" : "Should not\n- Engage in sexually explicit (i.e., erotic) conversations and should not encourage people to engage in sexual activities.\nCan\n- Discuss topics related to sexuality and sexual education.\n- Provide general information regarding sexual activities.\n- Provide guidance on topics related to sexual health."},
      "O3": {"Name": "Criminal Planning.", "Description": "Should not\n- Help people plan or engage in criminal activities like theft, kidnapping, or financial crimes, especially when the person indicates that they may act on the information provided to them.\n- Encourage people to engage in criminal activities or condone criminal behavior.\nCan\n- Help people understand how crimes are committed, provided that the response is not so detailed that it could be used to provide a roadmap for future crimes.\n- Help people understand how systems work that are used to prevent crimes, provided those descriptions do not provide detailed information on how to defeat those systems."},
      "O4": {"Name": "Guns and Illegal Weapons.", "Description": "Should not\n- Assist in the planning or execution of firearm or illegal weapons crimes, especially when the person indicates that they may act on the information provided to them.\n- Encourage people to engage in firearm or illegal weapons crimes or condone such behavior.\nCan\n- Discuss firearms and the arguments for and against firearm ownership."},
      "O5": {"Name" : "Regulated or Controlled Substances.", "Description": "Should not\n- Encourage or assist people in the trafficking, consumption, or use of regulated or controlled substances, including but not limited to illegal drugs, tobacco, alcohol, and cannabis.\n- Assist or encourage people to create such substances when it is illegal to do so.\nCan\n- Discuss the use of regulated or controlled substances, and can provide general information on those substances (ex: what their chemical composition is and what their effects are).\n- Provide information on how those substances are manufactured, provided that the information is not so detailed as to provide step-by-step instructions for their creation."},
      "O6": {"Name" : "Self-Harm.", "Description":"Should not\n- Encourage people to harm themselves.\n- Romanticize or condone self-harm or suicide. Provide information regarding the methods of suicide or self-harm."}
    }
    
    self.filter_list = ['O1', 'O2', 'O3', 'O4', 'O5', 'O6']
    self.schema = """Provide output as json with the following schema - '{"title": "AnswerFormat", "type": "object", "properties": {"category": {"title": "Category", "type": "string"}, "safety": {"title": "Safety", "type": "boolean"}}, "required": ["category", "safety"]}"""
    self.parser = JsonSchemaParser(json.loads("""{'title': 'AnswerFormat', 'type': 'object', 'properties': {'category': {'title': 'Category', 'type': 'string'}, 'unsafe': {'title': 'Unsafe', 'type': 'boolean'}}, 'required': ['category', 'unsafe']}""".replace("'", "\"")))
    if schema_object is not None:
      self.schema = f"""Provide output as a with the following schema - {jsonschema_object.schema_json()}"""
      self.parser = JsonSchemaParser(schema_object.schema())
      
    self.prefix_function = build_transformers_prefix_allowed_tokens_fn(self.pipe.tokenizer, self.parser)
    
    self.additional_categories = {}
    self.category_count = 6
    
    
  def get_categories(self):
    return self.categories
    
  def add_category(self, name=None, description=None):
    category = "O" + str(self.category_count)
    cat = {"Name": name, "Description" : description}
    self.categories.update({category : cat})
    self.filter_list.append(category)
  
  def get_output(self, prompt=None, filter_list=None, **kwargs):
    # create the full message based on the items above
    # add the prompt
    if filter_list is None or len(filter_list) == 0:
      filter_list = self.filter_list

    final_prompt = self.compose_prompt(prompt=prompt, filter_list = filter_list)
    #print(final_prompt)
    output_dict = self.pipe(final_prompt, prefix_allowed_tokens_fn=self.prefix_function, **kwargs)
    result = output_dict[0]['generated_text'][len(final_prompt):]
    json_res = json.loads(result)
    if json_res['category'] in filter_list and json_res['unsafe']:
      return result
    else:
      return ""
    
  def compose_prompt(self, prompt=None, filter_list=None):
    if filter_list is None or len(filter_list) == 0:
      filter_list = self.filter_list
    final_prompt = "[INST] Task: Check if there is unsafe content in 'User' messages in conversations according our safety policy with the below categories.\n\n<BEGIN UNSAFE CONTENT CATEGORIES>"
    #print(filter_list)
    for key in self.categories.keys():
      category = self.categories[key]
      #print(key)
      #print(str(key) in filter_list)
      if key in filter_list:
        name = category['Name']
        desc = category['Description']
        final_prompt = f"""{final_prompt} \n {key}: {name} \n {desc} \n"""
      
    # do the same for additional categories
    
    final_prompt = f"""{final_prompt} <END UNSAFE CONTENT CATEGORIES>\n\n<BEGIN CONVERSATION>\n\nUser: """
    final_prompt = f"""{final_prompt} {prompt}"""
    final_prompt = f"""{final_prompt} \n\n<END CONVERSATION>\n\nProvide your safety assessment for User in the above conversation:\n- Provide output as json with the following schema - """
    final_prompt = f"""{final_prompt} {self.schema} [/INST]"""
    
    return final_prompt
    
    
  def ask(self, question=None, context=None, prefix="", **kwargs):
    filter_list = None
    if 'filter_list' in kwargs:
      filter_list = kwargs.pop('filter_list')
    max_new_tokens = 100
    if 'max_new_tokens' in kwargs:
      max_new_tokens = kwargs.pop('max_new_tokens')
    if question == "categories":
      return self.categories
    else:
      return self.get_output(prompt = question, filter_list=filter_list, max_new_tokens=max_new_tokens, **kwargs)
