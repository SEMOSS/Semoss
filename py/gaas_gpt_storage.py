from gaas_server_proxy import ServerProxy

class StorageEngine(ServerProxy):
  
  def __init__(self, engine_id=None, insight_id=None):
    assert engine_id is not None 
    super().__init__()
    self.engine_id = engine_id
    self.insight_id = insight_id
    print("initialized")
   
  def list(self, path=None, insight_id=None):
    assert path is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    return super().call(
                      epoc = epoc, 
                      engine_type='Storage', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='list', 
                      method_args=[path],
                      method_arg_types=['java.lang.String']
                      )
    

  def listDetails(self, path=None, insight_id=None):
    assert path is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    return super().call(
                      epoc = epoc, 
                      engine_type='Storage', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='listDetails', 
                      method_args=[path],
                      method_arg_types=['java.lang.String']
                      )


  def syncLocalToStorage(self, localPath=None, storagePath=None, insight_id=None):
    assert localPath is not None
    assert storagePath is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    return super().call(
                      epoc = epoc, 
                      engine_type='Storage', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='syncLocalToStorage', 
                      method_args=[localPath, storagePath],
                      method_arg_types=['java.lang.String', 'java.lang.String']
                      )


  def syncStorageToLocal(self, localPath=None, storagePath=None, insight_id=None):
    assert localPath is not None
    assert storagePath is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    return super().call(
                      epoc = epoc, 
                      engine_type='Storage', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='syncStorageToLocal', 
                      method_args=[storagePath, localPath],
                      method_arg_types=['java.lang.String', 'java.lang.String']
                      )

  def copyToLocal(self, storageFilePath=None, localFolderPath=None, insight_id=None):
    assert storageFilePath is not None
    assert localFolderPath is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    return super().call(
                      epoc = epoc, 
                      engine_type='Storage', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='copyToLocal', 
                      method_args=[storageFilePath, localFolderPath],
                      method_arg_types=['java.lang.String', 'java.lang.String']
                      )


  def deleteFromStorage(self, storagePath=None, insight_id=None):
    assert storagePath is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    return super().call(
                      epoc = epoc, 
                      engine_type='Storage', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='deleteFromStorage', 
                      method_args=[storagePath],
                      method_arg_types=['java.lang.String']
                      )

  def deleteFromStorage(self, storagePath=None, leaveFolderStructure=False, insight_id=None):
    assert storagePath is not None
    if insight_id is None:
      insight_id = self.insight_id
    assert insight_id is not None
    epoc = super().get_next_epoc()
    return super().call(
                      epoc = epoc, 
                      engine_type='Storage', 
                      engine_id=self.engine_id, 
                      insight_id=insight_id, 
                      method_name='deleteFromStorage', 
                      method_args=[storagePath, leaveFolderStructure],
                      method_arg_types=['java.lang.String', 'java.lang.Boolean']
                      )

  