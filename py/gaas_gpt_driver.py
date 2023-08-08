class Driver():
  def __init__(self, insight_id=None):
    self.insight_id = insight_id
  

  def run_questions(self, question=None, engine_id="EMB_30991037-1e73-49f5-99d3-f28210e6b95c11", num_iterations=1):
    import gaas_gpt_model as ggm
    me = ggm.ModelEngine(engine_id=engine_id)
    response = []
    for ask_iter in range(0, num_iterations):
      answer = me.ask(question=question, insight_id=self.insight_id)
      response.append(answer)
    return response