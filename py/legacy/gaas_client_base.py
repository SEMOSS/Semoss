
import json
from string import Template

class BaseClient():
  # loads all the templates
  # fills the templates and gives information back
  def __init__(self,template_file="chat_templates.json"):
    self.templates=None
    self.template_file = template_file
    if template_file is not None:
      with open(template_file) as da_file:
        file_contents = da_file.read()
        self.templates = json.loads(file_contents)
    else:
      self.templates = {}
    print("Templates loaded")

  def get_template(self, template_name=None):
    if template_name in self.templates.keys():
      return self.templates[template_name]
    else:
      return None
      
  def add_template(self, template_name=None, template=None):
    assert template_name is not None
    if template_name not in self.templates:
      self.templates.update({template_name:template})
      print("template is set")
    else:
      print("template already exists")
      
  def write_templates(self, template_file=None):
    if template_file is None:
      template_file = self.template_file
      
    with open(template_file, 'w') as f:
      json.dump(self.templates, f)
      
  def fill_template(self, template_name=None, **args):
    this_template = self.get_template(template_name)
    if this_template is None:
      this_template=template_name
      
    if this_template is not None:
      template = Template(this_template)
      #mapping = {'name': 'John Doe', 'site': 'StackAbuse.com'}
      output = template.substitute(**args)
      return output
    else:
      return None
