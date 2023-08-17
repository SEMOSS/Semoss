from gaas_server_proxy import ServerProxy

class ModelEngine(ServerProxy):
  
  def __init__(self, engine_id=None, insight_id=None):
    assert engine_id is not None 
    super().__init__()
    self.engine_id = engine_id
    self.insight_id = insight_id
    print("initialized")
   
  def embeddings(self, question=None, insight_id=None, param_dict=None):
    assert question is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    return super().call(
                      epoc = epoc, 
                      engine_type='Model', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='embeddings', 
                      method_args=[question, insight_id, param_dict],
                      method_arg_types=['java.lang.String', 'prerna.om.Insight', 'java.util.Map']
                      )
    
    
  def ask(self, question=None, context=None, insight_id=None, param_dict=None):
    assert question is not None
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
    
    
  