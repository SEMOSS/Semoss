from typing import (
    List, 
    Optional, 
    Dict,
    Any
)

from gaas_server_proxy import ServerProxy

class ModelEngine(ServerProxy):
  
    def __init__(
        self, 
        engine_id: str, 
        insight_id: Optional[str] = None,
        local: Optional[bool]=False,
        pipeline_type: Optional[str]=None
    ):
        super().__init__()
        self.engine_id = engine_id
        self.insight_id = insight_id
        self.local = local
        
        if local:
          # start the model and make it available
          import torch
          from transformers import pipeline 
          device = "cpu"
          if torch.cuda.is_available():
            device = "cuda"
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
    
    def model(
        self,
        input: Any,
        insight_id: Optional[str] = None, 
        param_dict: Optional[Dict] = None,
    ):
        if not self.local:
          if insight_id is None:
              insight_id = self.insight_id
          assert insight_id is not None
          
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