
from ctransformers import AutoTokenizer, AutoModelForCausalLM
from transformers import TextStreamer, TextIteratorStreamer
from threading import Thread

from gaas_streamer import SemossStreamer
import huggingface_hub as hub_api
import os
from pathlib import Path



#import gaas_interrogator as gi
# i = gi.Interrogator()

class Interrogator():
  
  # LLama2 70b model - 'TheBloke/Llama-2-70B-Orca-200k-GGUF'
#'TheBloke/gorilla-7B-GGML' 'Gorilla-7B.ggmlv3.q8_0.bin'
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
    if "hf" not in kwargs:
      kwargs.update({"hf":True})

    # try to see if the serialized model is available
    # already ggml not required
    #self.model_folder = self.get_physical_folder(repo_id=model_path)
    #print(self.model_folder)
    #if self.model_folder is not None:
    #  self.cached_model_path = f"{self.model_folder}/ser"
    #  if os.path.exists(self.cached_model_path):
        # change to the model path
    #    self.model_name = self.cached_model_path
        #print(f"New load path {self.model_name}")
    #    self.serialized = True
    
    if autoload:
      print("loading model...", end="")
      self.load_model(self.model_name, **kwargs)
      print("loading tokenizer.. ", end="")
      self.load_tokenizer(self.model_name)
      print("..done")
    
  # mosaicml/mpt-7b-chat
  # EleutherAI/gpt-neox-20b
  #i2.load_model(model_path="mosaicml/mpt-7b-chat", model_loader=AutoModelForCausalLM, trust_remote_code=True)
  # orca mini-3b - "psmathur/orca_mini_3b",
  # it seems to safely go upto 40
  def load_model(self, model_path=None, model_loader=AutoModelForCausalLM, **kwargs):
    assert model_path is not None
    load_cuda = False
    #if 'device_map' in kwargs:
    #  kwargs.pop('device_map')
    #  kwargs.update({'gpu_layers':40})
    self.model = model_loader.from_pretrained(model_path, **kwargs)
    
  def load_tokenizer(self, model_path=None, tokenizer_loader=AutoTokenizer):
    tokenizer = AutoTokenizer.from_pretrained(self.model)
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
    
  def ask(self, text = None, question=None, prefix="", **kwargs):
    if text is None:
      text = question
    assert text is not None
    #print(self.model.device)
    tok_input = self.tokenizer(text, return_tensors="pt").to(self.model.device)
    input_ids = tok_input.input_ids.to(self.model.device)
    tok_input.attention_mask.to(self.model.device)
    console_streamer = SemossStreamer(tokenizer=self.tokenizer, skip_prompt=True)
    console_streamer.set_output_prefix(prefix)
    kwargs.update({"streamer":console_streamer})
    #print(kwargs)
    new_kwargs = self.configure_params(input_ids, **kwargs)
    
    #print(new_kwargs)
    #print(new_kwargs)
    #print("starting thread")
    #t = Thread(target=self.model.generate, kwargs=new_kwargs)
    #t.start()
    
    self.model.generate(**new_kwargs)
    model_output = ""
    #for new_text in self.console_streamer:
    #  model_output += new_text
    #  #print(new_text, end="")
    #  yield model_output
    #return model_output
