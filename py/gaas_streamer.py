from transformers import TextStreamer
import threading

class SemossStreamer(TextStreamer):

  def __init__(self, **kwargs):
    self.complete_output = ""
    super().__init__(**kwargs)

  def set_output_prefix(self, output_prefix):
    self.output_prefix = output_prefix

  # copied from here https://github.com/huggingface/transformers/blob/main/src/transformers/generation/streamers.py
  def on_finalized_text(self, text: str, stream_end: bool = False):
    """Prints the new text to stdout. If the stream is ending, also prints a newline."""
    self.complete_output += text
    if not stream_end:
      #print(self.output_prefix + text, flush=True, end="" if not stream_end else None)
      print(self.output_prefix + text, flush=True, end="")
    else:
      print(self.output_prefix + text, flush=True, end="")
      print(self.output_prefix + "D.O.N.E", flush=True, end="")
    
    #print(text, end="")
    #if stream_end:
    #  return