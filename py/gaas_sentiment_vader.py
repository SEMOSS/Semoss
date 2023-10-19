from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

class Sentiment():
  def __init__(self):
    self.engine = SentimentIntensityAnalyzer();
  
  def execute(self, input_arr=None, **kwargs):
    nltk.download('vader_lexicon')
    if not isinstance(input_arr, list):
      new_input = []
      new_input.append(input_arr)
      input_arr = new_input
    output_arr = []
    for sent in input_arr:
      polarity = self.engine.polarity_scores(sent)
      output_arr.append(polarity)
    return output_arr
    
  