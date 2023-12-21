
from transformers import AutoModel, AutoTokenizer, TextStreamer, AutoModelForCausalLM, TextIteratorStreamer
from threading import Thread

from gaas_streamer import SemossStreamer
import huggingface_hub as hub_api
import os
from pathlib import Path



#import gaas_interrogator as gi
# i = gi.Interrogator()

class Interrogator():
  
  
  def __init__(self, model_path=None, autoload=True, stopper="abracadabra", **kwargs):
    self.model_name=model_path
    self.snapshot="main"
    self.output_prefix = ""
    self.serialized = False
    self.stopper = stopper
    if "output_prefix" in kwargs:
      self.output_prefix = kwargs.pop("output_prefix")
    if "revision" in kwargs:
      self.snapshot = kwargs.pop("revision")
      
    # try to see if the serialized model is available
    self.model_folder = self.get_physical_folder(repo_id=model_path)
    #print(self.model_folder)
    if self.model_folder is not None:
      self.cached_model_path = f"{self.model_folder}/ser"
      if os.path.exists(self.cached_model_path):
        # change to the model path
        self.model_name = self.cached_model_path
        #print(f"New load path {self.model_name}")
        self.serialized = True
    
    if autoload:
      print("loading tokenizer.. ", end="")
      self.load_tokenizer(self.model_name)
      print("..done")
      print("loading model...", end="")
      self.load_model(self.model_name, **kwargs)
      print("..done")
    
  # mosaicml/mpt-7b-chat
  # EleutherAI/gpt-neox-20b
  #i2.load_model(model_path="mosaicml/mpt-7b-chat", model_loader=AutoModelForCausalLM, trust_remote_code=True)
  # orca mini-3b - "psmathur/orca_mini_3b",
  def load_model(self, model_path=None, model_loader=AutoModelForCausalLM, **kwargs):
    assert model_path is not None
    self.model = model_loader.from_pretrained(model_path, **kwargs)

  def load_model_cache(self, model_path=None, model_loader=AutoModelForCausalLM, **kwargs):
    assert model_path is not None
    self.model = model_loader.from_pretrained(model_path, **kwargs)

    
  def load_tokenizer(self, model_path=None, tokenizer_loader=AutoTokenizer):
    assert model_path is not None
    tokenizer = tokenizer_loader.from_pretrained(model_path)
    self.set_tokenizer(tokenizer)
    
  def set_model(self, model=None):
    assert model is not None
    self.model = model

  def set_tokenizer(self, tokenizer=None, remove_prompt=True):
    assert tokenizer is not None
    self.tokenizer = tokenizer
    #self.console_streamer = TextStreamer(self.tokenizer, skip_prompt=remove_prompt)
    #self.console_streamer = SemossStreamer(self.tokenizer, skip_prompt=remove_prompt)
    #self.console_streamer.set_output_prefix(self.output_prefix)
    #self.console_streamer = TextIteratorStreamer(self.tokenizer, skip_prompt=remove_prompt)
    #print(self.console_streamer)

  def overlay_lora(self, model=None, lora=None):
    from peft import PeftModel
    from transformers import LlamaTokenizer, LlamaForCausalLM, GenerationConfig, AutoModelForCausalLM

    #this for dolly, alpaca 13b
    self.model = PeftModel.from_pretrained(model, lora,device_map={'': 0})
    #model = PeftModel.from_pretrained(model, alpaca_model)
    return self.model
  
  def configure_params(self, input_ids, temperature=0.0, max_new_tokens = 100, do_sample=True, top_p=0, top_k=0, repetition_penalty=1.1, streamer=None):
    #if streamer is None:
    #  streamer = self.console_streamer # print out to the output      
    kwarg_dict = dict(
        input_ids=input_ids,
        max_new_tokens=max_new_tokens,
        temperature=temperature,
        #do_sample=temperature > 0.0,
        top_p=top_p,
        top_k=top_k,
        repetition_penalty=repetition_penalty,
        streamer=streamer,
    )
    return kwarg_dict
    
  def ask(self, text=None, **kwargs):
    assert text is not None
    #print(self.model.device)
    tok_input = self.tokenizer(text, return_tensors="pt").to(self.model.device)
    input_ids = tok_input.input_ids.to(self.model.device)
    tok_input.attention_mask.to(self.model.device)
    #print(kwargs)
    
    new_kwargs = self.configure_params(input_ids, **kwargs)
    
    #print(new_kwargs)
    #print(new_kwargs)
    #print("starting thread")
    #t = Thread(target=self.model.generate, kwargs=new_kwargs)
    #t.start()
    
    model_output = self.model.generate(**new_kwargs)
    
    num_input_tokens = tok_input.input_ids.size(1)
    output = model_output[0][num_input_tokens:]
    response = self.tokenizer.decode(output, skip_special_tokens=True)
    
    return {
        'response': response,
        'numberOfTokensInPrompt': num_input_tokens,
        'numberOfTokensInResponse': len(output)
    }
    #for new_text in self.console_streamer:
    #  model_output += new_text
    #  #print(new_text, end="")
    #  yield model_output
    #return model_output

  def serialize_model(self):
    # find the hub directory
    # Get the model name
    # check the directory to see if it is available
    # Create a pickle directory
    # drop it into the pickle directory
    # I am not accomodating for revisions yet.. should I ?
    import huggingface_hub as hub_api
    # base_folder = hub_api.dump_environment_info()['HUGGINGFACE_HUB_CACHE']
    # once snapshot has been downloaded
    # convert it into pickle
    if self.model_name is not None and self.model is not None:
      path = Path(hub_api.try_to_load_from_cache(repo_id=self.model_name, revision=self.snapshot, filename='config.json'))
      self.cached_model_path = path
      self.model_folder = path.parent.absolute()
      self.pickle_folder = f"{self.model_folder}/ser"
      if not os.path.isdir(self.pickle_folder):
        self.tokenizer.save_pretrained(self.pickle_folder)
        self.model.save_pretrained(self.pickle_folder)
    
  def download_repo(self, repo_id=None, snapshot='main', ignore_latest=True):
    assert repo_id is not None
    from huggingface_hub import hf_hub_download, snapshot_download, cached_download, hf_hub_url
    import huggingface_hub as hub_api
    if not self.check_if_snapshot_available(repo_id=repo_id,snapshot=snapshot): 
      snapshot_download(repo_id=repo_id, revision=snapshot)
    
  def check_if_snapshot_available(self, repo_id=None, snapshot='main'):
    config_file =  hub_api.try_to_load_from_cache(repo_id=repo_id, revision=snapshot, filename='config.json')
    return config_file is not None
    
    
  def get_physical_folder(self, repo_id=None, snapshot='main'):
    config_file =  hub_api.try_to_load_from_cache(repo_id=repo_id, revision=snapshot, filename='config.json')
    if config_file is not None:
      path = Path(hub_api.try_to_load_from_cache(repo_id=self.model_name, revision=self.snapshot, filename='config.json'))
      self.model_folder = path.parent.absolute()
      if self.model_folder is not None and os.path.exists(self.model_folder):
        return self.model_folder
      else:
        return None
    else:
      return None
      
  def is_serialized_model(self):
    return self.serialized
    
  