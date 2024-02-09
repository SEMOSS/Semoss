from typing import (
    List, 
    Optional, 
    Dict,
    Any
)
from abc import (
    ABC,
    abstractmethod
)
import os

from gaas_server_proxy import ServerProxy

class AbstractModelEngine(ABC):
    '''This is an abstract class the defined what methods need to be implemeted for a ModelEngine'''
    
    @abstractmethod
    def ask(self, *args: Any, **kwargs: Any) -> Dict:
        '''This method is responsible for interacting with models that can perform text-generation'''
        pass
    
    @abstractmethod
    def embeddings(self, *args: Any, **kwargs: Any) -> Dict:
        '''This method is responsible for interacting with models that can create embeddings from strings'''
        pass
    
    @abstractmethod
    def model(self, *args: Any, **kwargs: Any) -> Any:
        '''This method is responsible for utilizing a specific model function that is unique to that model function'''
        pass

    @abstractmethod
    def do_call(self, method_name:str, input: Any, **kwargs: Any) -> Any:
        '''This method is responsible for utilizing a specific tokenize function that is unique to that tokenize function'''
        pass

    
class TomcatModelEngine(AbstractModelEngine, ServerProxy):
  
    def __init__(
        self, 
        engine_id: str, 
        insight_id: Optional[str] = None,
        local: Optional[bool]=False,
        pipeline_type: Optional[str]=None,
        **kwargs
    ):
        super().__init__()
        self.engine_id = engine_id
        self.insight_id = insight_id
        self.local = local
        self.pipe = None
        
        if local:
          # start the model and make it available
          import torch
          from transformers import pipeline 
          device = 'cuda' if torch.cuda.is_available() else 'cpu'
          self.pipeline_type = pipeline_type
          self.pipe = pipeline(pipeline_type, model=engine_id, device=device)
    
    def ask(
        self, 
        question: str, 
        context: Optional[str] = None, 
        insight_id: Optional[str] = None, 
        param_dict: Optional[Dict] = None
    ) -> Dict:
        if insight_id is None:
            insight_id = self.insight_id
        # should I assert for insight_id as well I think I should
        assert insight_id is not None    
        epoc = super().get_next_epoc()
        return super().call(
            epoc=epoc, 
            engine_type='Model', 
            engine_id=self.engine_id, 
            method_name='ask', 
            method_args=[question,context,insight_id, param_dict],
            method_arg_types=['java.lang.String', 'java.lang.String', 'prerna.om.Insight', 'java.util.Map'],
            insight_id = insight_id
        )
  
    def embeddings(
        self, 
        strings_to_embed: List[str], 
        insight_id: Optional[str] = None, 
        param_dict: Optional[Dict] = None
    ) -> Dict:
    
        if not self.local:
          if isinstance(strings_to_embed,str):
              strings_to_embed = [strings_to_embed]
          assert isinstance(strings_to_embed, list)
          if insight_id is None:
              insight_id = self.insight_id
          assert insight_id is not None
          epoc = super().get_next_epoc()
          return super().call(
              epoc=epoc, 
              engine_type='Model', 
              engine_id=self.engine_id, 
              insight_id=insight_id, 
              method_name='embeddings', 
              method_args=[strings_to_embed, insight_id, param_dict],
              method_arg_types=['java.util.List', 'prerna.om.Insight', 'java.util.Map']
          )
        else:
          return self.pipe.model.encode(strings_to_embed)
    
    def model(
        self,
        input: Any,
        insight_id: Optional[str] = None, 
        param_dict: Optional[Dict] = None,
    ):
        if not self.local:
          if insight_id is None:
              insight_id = self.insight_id
          #assert insight_id is not None
          
          epoc = super().get_next_epoc()
          return super().call(
              epoc=epoc, 
              engine_type='Model', 
              engine_id=self.engine_id, 
              insight_id=insight_id, 
              method_name='model', 
              method_args=[input, insight_id, param_dict],
              method_arg_types=['java.lang.Object', 'prerna.om.Insight', 'java.util.Map']
          )
        else:
          return self.pipe(input)

    def get_model_type(
        self, 
        insight_id: Optional[str] = None
    ):
        if not self.local:
          if insight_id is None:
              insight_id = self.insight_id
          epoc = super().get_next_epoc()
          return super().call(
              epoc=epoc, 
              engine_type='Model', 
              engine_id=self.engine_id, 
              insight_id=insight_id, 
              method_name='getModelType', 
              method_args=[],
              method_arg_types=[]
          )
        else:
          return self.pipeline_type
          
    # this is a little bit of get out of jail free card
    def do_call(
        self,
        method_name: str,
        input: Any,
        **kwargs) -> Any:
      call_maker = getattr(self, method_name, None) 
      if call_maker is not None:
        return call_maker(input, **kwargs)
      else:
        return None
        
      
class LocalModelEngine(AbstractModelEngine):
  
    def __init__(
        self,
        model_engine: Any = None, 
        engine_id: Optional[str] = None,
        engine_smss_file_path: Optional[str] = None,
        semoss_dev_path: Optional[str] = 'C:/users/pkapaleeswaran/workspacej3/SemossDev' if os.name == 'nt' else '/opt/semosshome',
    ):

        # determine how to create the model engine locally
        if engine_smss_file_path is not None or engine_id is not None:
            
            if engine_smss_file_path is not None:
                # the direct path of the smss file was passed in
                pass
            elif engine_id is not None:
                # the gave the engine id so try to find the smss file
                engine_smss_file_path = LocalModelEngine.get_model_smss_file(
                    semoss_dev_path, 
                    engine_id
                )
            
            # get the smss props from the smss file
            smss_props = LocalModelEngine.read_smss_file(engine_smss_file_path)
            
            # use the smss props to initialize the model engine
            model_engine_init_command = LocalModelEngine.get_init_model_commads(smss_props)
            exec(model_engine_init_command)
            
            self.local_model_engine = locals().get(smss_props['VAR_NAME'], None)
        else:
            self.local_model_engine = model_engine
            
        assert self.local_model_engine != None, "Unable to define a Local Model Engine based on the parameters passed in"
    
    def ask(
        self, 
        **kwargs
    ) -> Dict:
        return self.local_model_engine.ask(**kwargs)
  
    def embeddings(
        self, 
        **kwargs
    ) -> Dict:
        return self.local_model_engine.embeddings(**kwargs)
    
    def model(
        self,
        **kwargs
    ):
        return self.local_model_engine.model(**kwargs)

    def get_model_type(
        self,
        **kwargs
    ):
        #TODO add model type in python as well
        return None
        
    def do_call(
        self,
        method_name: str,
        input: Any,
        **kwargs) -> Any:
      call_maker = getattr(self, method_name, None) 
      if call_maker is not None:
        return call_maker(input, **kwargs)
      else:
        return None

    
    @staticmethod
    def get_model_smss_file(semoss_dev_file_path:str, engine_id:str) -> str:
        '''This method returns a list of smss files in the semosshome model directory'''
        import glob
        
        file_pattern = '*.smss'

        # Use glob.glob to find all matching files in the directory
        model_smss_files = glob.glob(
            os.path.join(semoss_dev_file_path, 'model', file_pattern)
        )
        model_smss_files.sort()
        
        for smss_file_name in model_smss_files:
            if smss_file_name.find(engine_id) > 0:
                return smss_file_name
            
        raise ValueError(f'Unable to find smss file for the engine id: {engine_id}')

        
    @staticmethod
    def read_smss_file(file_path:str) -> Dict:
        smss_props = {}
        with open(file_path.replace('\\','/'), 'r') as file:
            for line in file:
                line = line.strip()
                if line.startswith('#') or len(line) == 0:
                    continue
                try:
                    key, value = line.split(None, 1)  # Split by any whitespace
                except:
                    pass
                smss_props[key] = True if value.lower() == 'true' else (False if value.lower() == 'false' else value.replace('\\','/'))
        return smss_props

    @staticmethod
    def get_init_model_commads(smss_props:dict) -> str:
        import re

        init_model_engine_template = smss_props['INIT_MODEL_ENGINE']

        # Find all placeholders in the template string
        placeholders = re.findall(r'\${(.*?)}', init_model_engine_template)

        # Create a dictionary with the actual values for the found placeholders
        values = {placeholder: smss_props[placeholder] for placeholder in placeholders if placeholder in smss_props}

        # Substitute the placeholders with the actual values from the dictionary
        formatted_string_dynamic = init_model_engine_template
        for placeholder, value in values.items():
            formatted_string_dynamic = formatted_string_dynamic.replace('${' + placeholder + '}', value)

        return formatted_string_dynamic
    
class ModelEngine(AbstractModelEngine):
    
    def __init__(
        self,
        local: Optional[bool] = False,
        **kwargs
    ):
        if local:
            self.model_engine = LocalModelEngine(**kwargs)
        else:
            self.model_engine = TomcatModelEngine(**kwargs)
            
        assert self.model_engine != None, "Unable to define a Model Engine"
        
    def ask(
        self, 
        **kwargs
    ) -> Dict:
        return self.model_engine.ask(**kwargs)
  
    def embeddings(
        self, 
        **kwargs
    ) -> Dict:
        return self.model_engine.embeddings(**kwargs)
    
    def model(
        self,
        **kwargs
    ):
        return self.model_engine.model(**kwargs)

    def get_model_type(
        self, 
        **kwargs
    ):
        return self.model_engine.get_model_type(**kwargs)
     
    def do_call(
        self, 
        method_name:str, 
        input: Any, 
        **kwargs: Any) -> Any:
      return self.model_engine.embeddings(**kwargs)
