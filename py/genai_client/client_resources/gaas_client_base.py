
import json
from string import Template
import os

class BaseClient():
  chat_templates = {
      "orca.default.context":"### System:\n$system\n\n### User:\n$question\n\n### Input:\n$context\n\n### Response:\n",
      "orca.default.nocontext":"### System:\n$system\n\n### User:\n$question\n\n### Response:\n",
      "guanaco.default.nocontext": "Below is an instruction that describes a task. Write a response that appropriately completes the request.\n\n###",
      "guanaco.default.context": "A chat between a curious human and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions. Based on the following paragraphs, answer the human's question:\n\n",
      "wizard.default.nocontext": "Below is an instruction that describes a task. Write a response that appropriately completes the request.\n\n### Instruction: $question\n\n### Response:",
      "wizard.default.context": "A chat between a curious human and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions. USER: $context. $question ? ASSISTANT:",
      "sql.default.context": "A chat between a curious human and an artificial intelligence assistant. The assistant gives helpful, detailed, and polite answers to the user's questions. Based on the table and columns defined below, create a sql statement that answers the human's question:### Question:\n\n$question\n\n### SQL:"
  }
  # loads all the templates
  # fills the templates and gives information back
  def __init__(self,template_file=None):
    self.templates=None
    if (template_file == None):
      self.template_file = self.chat_templates
      self.templates = self.chat_templates

  def get_template(self, template_name=None):
    if template_name in self.templates.keys():
      return self.templates[template_name]
    else:
      return None
      
  def add_template(self, template_name=None, template=None):
    assert template_name is not None
    if template_name not in self.templates:
      self.templates.update({template_name:template})
      
    return True
      
  def write_templates(self, template_file=None):
    if template_file is None:
      template_file = self.template_file
      
    with open(template_file, 'w') as f:
      json.dump(self.templates, f)
      
  def fill_template(self, template_name=None, **args):
    assert template_name is not None
    this_template = self.get_template(template_name)
    if this_template is not None:
      template = Template(this_template)
      #mapping = {'name': 'John Doe', 'site': 'StackAbuse.com'}
      output = template.substitute(**args)
      return output
    else:
      return None
