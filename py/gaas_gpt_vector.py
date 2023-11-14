from gaas_server_proxy import ServerProxy
from typing import List, Tuple, Dict
import os

class VectorEngine(ServerProxy):
    
    engine_type = 'VECTOR'
    
    def __init__(
        self,
        insight_folder:str,
        engine_id:str = None, 
        insight_id:str = None,
    ):
        super().__init__()
        self.engine_id = engine_id
        self.insight_id = insight_id
        self.insight_folder = insight_folder
        
    def addDocument(
        self,
        file_paths:List[str],
        engine_id:str = None, 
        insight_id:str = None,
        param_dict:Dict = {}
    ):
        engine_id, insight_id = self._determine_ids(
            engine_id = engine_id,
            insight_id = insight_id
        )
        
        param_dict['insight'] = insight_id
        
        for i, file_path in enumerate(file_paths):
            if os.path.isfile(file_path):
                file_paths[i] = file_paths.replace('\\','/')
            else:
                updated_file_path = os.path.join(
                    self.insight_folder, 
                    file_path
                )
                file_exists = os.path.isfile(updated_file_path)
                if file_exists:
                    file_paths[i] = updated_file_path.replace('\\','/')
                else:
                    raise IOError(f'Unable to find file path for {file_path}')
        
        epoc = super().get_next_epoc()
        return super().call(
            epoc = epoc, 
            engine_type = VectorEngine.engine_type, 
            engine_id = engine_id, 
            method_name = 'addDocument', 
            method_args=[file_paths, param_dict],
            method_arg_types=['java.util.List', 'java.util.Map'],
            insight_id = insight_id
        )[0]
            
    def removeDocument(
        self,
        file_names:List[str],
        engine_id:str = None, 
        insight_id:str = None,
        param_dict:Dict = {}
    ):
        engine_id, insight_id = self._determine_ids(
            engine_id = engine_id,
            insight_id = insight_id
        )
        
        param_dict['insight'] = insight_id
        
        epoc = super().get_next_epoc()
        return super().call(
            epoc = epoc, 
            engine_type = VectorEngine.engine_type, 
            engine_id = engine_id, 
            method_name = 'removeDocument', 
            method_args=[file_names, param_dict],
            method_arg_types=['java.util.List', 'java.util.Map'],
            insight_id = insight_id
        )[0]
    
    def nearestNeighbor(
        self,
        search_statement:str,
        limit:int = 5,
        param_dict:Dict = {},
        engine_id:str = None, 
        insight_id:str = None,
    ):
        engine_id, insight_id = self._determine_ids(
            engine_id = engine_id,
            insight_id = insight_id
        )
        
        param_dict['insight'] = insight_id

        epoc = super().get_next_epoc()
        return super().call(
            epoc = epoc, 
            engine_type = VectorEngine.engine_type, 
            engine_id = engine_id, 
            method_name = 'nearestNeighbor', 
            method_args=[search_statement, limit, param_dict],
            method_arg_types=['java.lang.String', 'java.lang.Number', 'java.util.Map'],
            insight_id = insight_id
        )[0]
    
    def listDocuments(
        self,
        engine_id:str = None, 
        insight_id:str = None,
        param_dict:Dict = {}
    ):
        engine_id, insight_id = self._determine_ids(
            engine_id = engine_id,
            insight_id = insight_id
        )
        
        assert engine_id != None
        epoc = super().get_next_epoc()
        return super().call(
            epoc = epoc, 
            engine_type = VectorEngine.engine_type, 
            engine_id = engine_id, 
            method_name = 'listDocuments', 
            method_args=[param_dict],
            method_arg_types=['java.util.Map'],
            insight_id = insight_id
        )[0]
        
    def _determine_ids(
        self, 
        engine_id:str, 
        insight_id:str
    ) -> Tuple[str, str]:
        if engine_id == None:
            engine_id = self.engine_id
        
        if insight_id == None:
            insight_id = self.insight_id
            
        assert engine_id != None
        assert insight_id != None
        
        return engine_id, insight_id