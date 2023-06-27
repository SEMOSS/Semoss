
from transformers import AutoModel, AutoTokenizer, TextStreamer, AutoModelForCausalLM

#import gaas_interrogator as gi
# i = gi.Interrogator()

class Interrogator():
  
  
  def __init__(self, model_path=None, **kwargs):
    print("loading tokenizer.. ", end="")
    self.load_tokenizer(model_path)
    print("..done")
    print("loading model...", end="")
    self.load_model(model_path, **kwargs)
    print("..done")
  # mosaicml/mpt-7b-chat
  # EleutherAI/gpt-neox-20b
  #i2.load_model(model_path="mosaicml/mpt-7b-chat", model_loader=AutoModelForCausalLM, trust_remote_code=True)
  # orca mini-3b - "psmathur/orca_mini_3b",
  def load_model(self, model_path=None, model_loader=AutoModelForCausalLM, **kwargs):
    assert model_path is not None
    self.model = model_loader.from_pretrained(model_path, **kwargs)
    
  def load_tokenizer(self, model_path=None, tokenizer_loader=AutoTokenizer):
    assert model_path is not None
    tokenizer = tokenizer_loader.from_pretrained(model_path)
    self.set_tokenizer(tokenizer)
    
  def set_model(self, model=None):
    assert model is not None
    self.model = model

  def set_tokenizer(self, tokenizer=None):
    assert tokenizer is not None
    self.tokenizer = tokenizer
    self.console_streamer = TextStreamer(self.tokenizer)
    #print(self.console_streamer)

  def overlay_lora(self, model=None, lora=None):
    from peft import PeftModel
    from transformers import LlamaTokenizer, LlamaForCausalLM, GenerationConfig, AutoModelForCausalLM

    #this for dolly, alpaca 13b
    self.model = PeftModel.from_pretrained(model, lora,device_map={'': 0})
    #model = PeftModel.from_pretrained(model, alpaca_model)
    return self.model
  
  def configure_params(self, input_ids, temperature=0.0, max_new_tokens = 100, do_sample=True, top_p=0, top_k=0, repetition_penalty=1.1, streamer=None):
    if streamer is None:
      streamer = self.console_streamer # print out to the output      
    kwarg_dict = dict(
        input_ids=input_ids,
        max_new_tokens=max_new_tokens,
        temperature=temperature,
        do_sample=temperature > 0.0,
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
    new_kwargs = self.configure_params(input_ids, **kwargs)
    #print(new_kwargs)
    
    self.model.generate(**new_kwargs)

  