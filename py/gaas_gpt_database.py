from gaas_server_proxy import ServerProxy

class DatabaseEngine(ServerProxy):
  
  def __init__(self, engine_id=None, insight_id=None):
    assert engine_id is not None 
    super().__init__()
    self.engine_id = engine_id
    self.insight_id = insight_id
    print("initialized")
   
  def execQuery(self, query=None, insight_id=None, return_pandas=True):
    assert query is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    fileLoc = super().call(
                      epoc = epoc, 
                      engine_type='database', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='execQuery', 
                      method_args=[query],
                      method_arg_types=['java.lang.String']
                      )
    if isinstance(fileLoc, list) and len(fileLoc) > 0:
      fileLoc = fileLoc[0]
    try:
      if return_pandas:
        print(f"file Location {fileLoc}")
        import pandas as pd
        return pd.read_json(fileLoc)
      else:
        return open(fileLoc, "r").read()
    finally:
      # Always attempt to remove the file regardless of success
      import os
      if os.path.exists(fileLoc):
        os.remove(fileLoc)

  def insertData(self, query=None, insight_id=None):
    assert query is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    return super().call(
                      epoc = epoc, 
                      engine_type='database', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='insertData', 
                      method_args=[query],
                      method_arg_types=['java.lang.String']
                      )


  def removeData(self, query=None, insight_id=None):
    assert query is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    return super().call(
                      epoc = epoc, 
                      engine_type='database', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='removeData', 
                      method_args=[query],
                      method_arg_types=['java.lang.String']
                      )

  
    
