class Driver():
  def __init__(self, insight_id=None):
    self.insight_id = insight_id
  

  def run_model(self, question=None, engine_id="EMB_30991037-1e73-49f5-99d3-f28210e6b95c11", num_iterations=1):
    import gaas_gpt_model as ggm
    me = ggm.ModelEngine(engine_id=engine_id)
    response = []

    for ask_iter in range(0, num_iterations):
      answer = me.ask(question=question, insight_id=self.insight_id)[0]["response"]
      response.append(answer)
    return response
    
  
  def run_database(self, query=None, engine_id="09c41d13-5301-4e62-a225-9abac4ca5f4d", num_iterations=1, **kwargs):
    import gaas_gpt_database as ggd
    me = ggd.DatabaseEngine(engine_id=engine_id)
    response = []
    for ask_iter in range(0, num_iterations):
      answer = me.execQuery(query=query, insight_id=self.insight_id, **kwargs)
      response.append(answer)
    return response
    
    
  def run_storage(self, path=None, engine_id="aaafaa3d-ec1a-4109-8889-8b7e17470aaa", num_iterations=1, **kwargs):
    import gaas_gpt_storage as ggs
    se = ggs.StorageEngine(engine_id=engine_id)
    response = []
    for ask_iter in range(0, num_iterations):
      answer = se.listDetails(path=path, insight_id=self.insight_id, **kwargs)
      response.append(answer)
    return response