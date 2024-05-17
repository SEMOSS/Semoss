from gaas_server_proxy import ServerProxy

class DatabaseEngine(ServerProxy):
  
  def __init__(self, engine_id=None, insight_id=None):
    assert engine_id is not None 
    super().__init__()
    self.engine_id = engine_id
    self.insight_id = insight_id
    print(f"Engine {engine_id} is initialized")


  def execQuery(self, query=None, insight_id=None, return_pandas=True):
    assert query is not None
    if insight_id is None:
      insight_id = self.insight_id
    # assert insight_id is not None
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


  def insertData(
      self,
      query=None, 
      insight_id=None,
      commit:bool = True):
    '''
      This method is responsible for running a insert data into the database
      
      Args:
          query (`str`): The query to run against the database
          insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
          commit (`bool`): commit to the database if autocommit is false. default is true
      
      Returns:
          boolean: true/false if this ran successfully
    '''
    return self.runQuery(query, insight_id, commit)

  def updateData(
      self,
      query=None, 
      insight_id=None,
      commit:bool = True):
    '''
      This method is responsible for running a insert data into the database
      
      Args:
          query (`str`): The query to run against the database
          insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
          commit (`bool`): commit to the database if autocommit is false. default is true

      Returns:
          boolean: true/false if this ran successfully
    '''
    return self.runQuery(query, insight_id, commit)

  def removeData(
      self, 
      query=None, 
      insight_id=None,
      commit:bool = True):
    '''
      This method is responsible for removing data from the database
      
      Args:
          query (`str`): The query to run against the database
          insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
          commit (`bool`): commit to the database if autocommit is false. default is true

      Returns:
          boolean: true/false if this ran successfully
    '''
    return self.runQuery(query, insight_id, commit)
    

  def runQuery(
      self, 
      query=None, 
      insight_id=None,
      commit:bool = True):
    '''
      This method is responsible for running the exec query against the database
      
      Args:
          query (`str`): The query to run against the database
          insight_id (`Optional[str]`): Unique identifier for the temporal worksapce where actions are being isolated
          commit (`bool`): commit to the database if autocommit is false. default is true

      Returns:
          boolean: true/false if this ran successfully
    '''
    assert query is not None
    if insight_id is None:
      insight_id = self.insight_id
    
    commitStr = "true" if commit else "false"

    # assert insight_id is not None
    epoc = super().get_next_epoc()
    pixel = f'Database("{self.engine_id}")|Query("<encode>{query}</encode>")|ExecQuery(commit={commitStr});'
    pixelReturn = super().callReactor(
                      epoc = epoc,
                      pixel=pixel,
                      insight_id=insight_id, 
                      )
    
    if pixelReturn is not None and len(pixelReturn) > 0:
        output = pixelReturn[0]['pixelReturn'][0]
        return output['output']
    
    return pixelReturn
  
    
