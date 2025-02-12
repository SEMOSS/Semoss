from detoxify import Detoxify

#pip install detoxify

# from smss_util.Toxicity import ToxicityMaker
# t = ToxicityMaker()
# t.isThreat("he is going to kill him")



class ToxicityMaker:

  def __init__(self, model_type="original"):
    # try to see the repo type
    # load it accordingly
    # This code will assume Cuda
    # this also assumes the files are available locally i.e. it is not pulling the files
    # expects the files to be in the huggingface hub
    
    self.model = Detoxify(model_type)
  
  def getCategory(self, text_data=None, category=None, threshold=None):
    # convert prompt with context
    output = {}
    final_output = {}
    final_output.update({'prompt':text_data})
    final_output.update({'categories': category})
    final_output.update({'threshold': threshold})
    toxic_output = self.model.predict(text_data)
    if category is not None:
      for c in category:
        if c in toxic_output:
          toxic_val = toxic_output[c]
          cur_val = 0
          if threshold is not None and c in threshold:
            cur_val = threshold[c]
          if cur_val <= toxic_val:
            output.update({c:toxic_val})
      final_output.update({'toxicity':output})
    else:
      final_output.update({'toxicity': toxic_output})
      final_output.update({'prompt': text_data})
    
    return final_output
    
  def isToxic(self, text_data=None, threshold=None):
    category=['toxicity', 'severe_toxicity']
    if threshold is None:
      threshold = {'toxicity':0.5, 'severe_toxicity':0.5}
    return self.getCategory(text_data, category, threshold)
  
  def isObscene(self, text_data=None, threshold=None):
    category=['obscene']
    if threshold is None:
      threshold = {'obscene':0.5}
    return self.getCategory(text_data, category, threshold)
  
  def isInsult(self, text_data=None, threshold=None):
    category=['insult']
    if threshold is None:
      threshold = {'insult':0.5}
    return self.getCategory(text_data, category, threshold)
      
  def isThreat(self, text_data=None, threshold=None):
    category=['threat']
    if threshold is None:
      threshold = {'threat':0.5}
    return self.getCategory(text_data, category, threshold)
    
  def isIdentityAttack(self, text_data=None, threshold=None):
    category=['identity_attack']
    if threshold is None:
      threshold = {'identity_attack':0.5}
    return self.getCategory(text_data, category, threshold)
    