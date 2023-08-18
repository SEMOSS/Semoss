
import json
from string import Template

class BaseClient():
  # loads all the templates
  # fills the templates and gives information back
  def __init__(self,template_file="chat_templates.json"):
    self.templates= {}
    if (template_file != None):
      self.template_file = template_file
      with open(template_file) as da_file:
        file_contents = da_file.read()
        self.templates = json.loads(file_contents)
      print("Templates loaded")

  def get_template(self, template_name=None):
    if template_name in self.templates.keys():
      return self.templates[template_name]
    elif f"{self.model_name}.default.context" in self.templates:
      return self.templates[f"{self.model_name}.default.context"]
    elif f"{self.model_name}.default.nocontext" in self.templates:
      return self.templates[f"{self.model_name}.default.nocontext"]
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
      
  def fill_template(self, template_name=None, **kwargs):
    assert template_name is not None
    this_template = self.get_template(template_name)
    if this_template is not None:
      return self.fill_context(this_template, **kwargs)
    else:
      return None, False
  
  # not, kwargs here is just a dictionary -- not a dictionary construction
  def fill_context(self, theContext, **kwargs):
    template = Template(theContext)
    output = template.substitute(**kwargs)

    # assumption -- if str substitution occures, then we dont need to user,system prompt ourselves
    if output != theContext:
      substitutions_made = True
    else:
      substitutions_made = False

    return output, substitutions_made
