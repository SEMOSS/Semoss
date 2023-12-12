from typing import Optional, Union, List, Dict, Any
import json
import os
from string import Template
from abc import (
    ABC, 
    abstractmethod
)
from ..constants import (
    ModelEngineResponse
)

class BaseClient(ABC):
  # loads all the templates
  # fills the templates and gives information back
    def __init__(
        self, 
        template:Union[Dict, str] = None,
        template_name:str = None,
    ):
        self.template_name = template_name
        self.templates= {}

        # if the user does not provide a template, we default to chat_templates.json
        if (template == None):
            script_directory = os.path.dirname(os.path.abspath(__file__))
            chat_templates = os.path.join(script_directory, "chat_templates.json")
            template = chat_templates
      
        # the user should be able to pass, their own file (json) or dictionary
        if isinstance(template, str):
        # since its a string, we assume its path and need to validate that its valid
            if os.path.exists(template) == False:
                raise FileNotFoundError(f"The file '{template}' does not exist.")

            self.template_file = template
            with open(template) as da_file:
                file_contents = da_file.read()
                self.templates = json.loads(file_contents)
        elif isinstance(template, dict):
            self.template_file = None
            self.templates = template
    
    def get_template(self, template_name=None, **kwargs):
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
        this_template = self.get_template(template_name, **kwargs)
        if this_template is not None:
            return self.fill_context(this_template, **kwargs)
        else:
            return None, False
  
    # note, kwargs here is just a dictionary -- not a dictionary construction
    def fill_context(self, theContext, **kwargs):
        template = Template(theContext)
        output = template.substitute(**kwargs)
    # assumption -- if str substitution occures, then we dont need to user,system prompt ourselves
        if output != theContext:
            substitutions_made = True
        else:
            substitutions_made = False

        return output, substitutions_made
  
    @abstractmethod
    def ask(self, *args: Any, **kwargs: Any) -> Dict:
        pass

    def embeddings(self, *args: Any, **kwargs: Any) -> Dict:
        response = 'This model does not support embeddings.'
        
        numberOfTokensInResponse = 6
        try:
            self.tokenizer.count_tokens(response)
        except:
            pass
        
        output_payload = ModelEngineResponse(
            response:response,
            promptTokens: 0,
            responseTokens: numberOfTokensInResponse
        )
        
        return output_payload.to_dict()