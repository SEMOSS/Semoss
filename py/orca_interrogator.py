from transformers import AutoModel, AutoTokenizer, TextStreamer, AutoModelForCausalLM

import gaas_interrogator as gi

class Orca_Interrogator(gi.Interrogator):
  def __init__(self, model_path="psmathur/orca_mini_3b", **kwargs):
    print("Creating tokenizer and Model")
    super().__init__(model_path=model_path, **kwargs)
    print("<<Orca Ready>>")
  
  def ask(self, question=None, context=None, **kwargs):
    prompt = ""
    system='You are an AI assistant that follows instruction extremely well. Help as much as you can.'
    if context:
        prompt = f"### System:\n{system}\n\n### User:\n{question}\n\n### Input:\n{context}\n\n### Response:\n"
    else:
        prompt = f"### System:\n{system}\n\n### User:\n{question}\n\n### Response:\n"
    
    return super().ask(prompt, **kwargs)
  
