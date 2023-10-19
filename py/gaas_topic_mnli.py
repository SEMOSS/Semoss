from transformers import pipeline

class TopicEngine():
  def __init__(self):
    # nothing much to init here
    self.model = "MoritzLaurer/DeBERTa-v3-base-mnli-fever-docnli-ling-2c"
    self.pipe = pipeline("zero-shot-classification", self.model)
    
  def ask(self, question=None, **kwargs):
    # pop the topics from kwargs
    # call the pipe 
    # return result 
    topics = ["politics", "violence", "entertainment", "food"]
    if "topics" in kwargs:
      topics = kwargs.pop("topics")
    
    result = self.pipe(question, topics)
    return result

  def model(self, question=None, **kwargs):
    # pop the topics from kwargs
    # call the pipe 
    # return result 
    topics = ["politics", "violence", "entertainment", "food"]
    if "topics" in kwargs:
      topics = kwargs.pop("topics")
    
    result = self.pipe(question, topics)
    return result
