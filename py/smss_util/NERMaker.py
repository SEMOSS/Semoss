from gliner import GLiNER

import uuid

# nm = NM.NERMaker()
#nm.init(model_name="urchade/gliner_large-v2.1")

class NERMaker:

  def init(self, model_name=None, tokenizer_name=None, file_name=None, snapshot=None, repo_type=None, local_files_only=True, **kwargs):
    self.model = GLiNER.from_pretrained(model_name, local_files_only=local_files_only, **kwargs)
    
    
  def getEntities(self, text_data=None, entities=[]):
    
    output = {}
    if text_data is None:
      output.update({"input": "no data provided"})
      return output
    else:
      output.update({"input": text_data})

    if text_data is None:
      output.update({"input": "no data provided"})
      return output
    else:
      output.update({"input": text_data})
  
    if len(entities) is None:
      output.update({"entities": ["no entity provided"]})
      return output
    else:
      output.update({"entities": entities})
    
    predictions = self.model.predict_entities(text_data, entities)
    output.update({"raw_output": predictions})
    
    # process and organize by labels
    organizer = {}
    for prediction in predictions:
      label = prediction['label']
      label_values = []
      if label in organizer:
        label_values = organizer[label]
      
      label_values.insert(len(label_values), prediction['text'])
      organizer.update({label:label_values})
      
    output.update({"output": organizer})
    
    return output
  
  # mask the entities 
  def maskEntities(self, text_data=None, entities=[], mask_entities=[]):
  
    processed_input = self.getEntities(text_data, entities)
    
    # the dictionary that keeps from and to masking values
    masker = {}
    new_text = text_data
    
    if len(mask_entities) == 0:
      mask_entities = entities
    
    if "raw_output" in processed_input:
      raw_output = processed_input['raw_output']
      for item in raw_output:
        orig_text = item['text']
        start = item['start']
        end = item['end']
        label = item['label']
        if label in mask_entities:          
          mask_text = ""
          if orig_text in masker:
            mask_text = masker['orig_text']
          else:
            length = end - start
            mask_text = "m_" + self.makeRandom(N=length)
          
          masker.update({orig_text : mask_text})
          masker.update({mask_text: orig_text})
          
          # this is affecting length.. you need to make sure it is not affecting length else you need to re-run it
          new_text = new_text[:start] + " " + mask_text + " " + new_text[end:]
      
      processed_input.update({"output": new_text})
      processed_input.update({"mask_values": masker})

    else:
      processed_input.update({"output": " Unable to process "})
      
    return processed_input
    
  def makeRandom(self, N=6):
    import random
    import string
    ret_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))
    return ret_string
  