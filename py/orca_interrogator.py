from transformers import AutoModel, AutoTokenizer, TextStreamer, AutoModelForCausalLM

import gaas_interrogator as gi
from gaas_streamer import SemossStreamer

class Interrogator(gi.Interrogator):
  def __init__(self, model_path="psmathur/orca_mini_3b", autoload=True, stopper="abracadabra", **kwargs):
    print("Creating tokenizer and Model")
    super().__init__(model_path=model_path, autoload=autoload, stopper=stopper, **kwargs)
    print("<<Orca Ready>>")
  
  def ask(self, question=None, context=None, prefix="", **kwargs):
    prompt = ""
    system='You are an AI assistant that follows instruction extremely well. Help as much as you can.'
    if context:
        prompt = f"### System:\n{system}\n\n### User:\n{question}\n\n### Input:\n{context}\n\n### Response:\n"
    else:
        prompt = f"### System:\n{system}\n\n### User:\n{question}\n\n### Response:\n"
    console_streamer = SemossStreamer(tokenizer=self.tokenizer, skip_prompt=True)
    console_streamer.set_output_prefix(prefix)
    output = super().ask(prompt, streamer=console_streamer, **kwargs)
    #output = console_streamer.complete_output
    return {
        'response': output,
        'numberOfTokensInPrompt': len(self.tokenizer.encode(prompt)),
        'numberOfTokensInResponse': len(self.tokenizer.encode(output))
    }
  
