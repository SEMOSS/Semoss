from typing import List, Tuple, Dict, Optional
import os
import zipfile
import shutil

from gaas_server_proxy import ServerProxy

class VectorEngine(ServerProxy):
    
    engine_type = 'VECTOR'
    
    def __init__(
        self,
        insight_folder:str,
        engine_id:Optional[str] = None, 
        insight_id:Optional[str] = None,
    ):
        super().__init__()
        self.engine_id = engine_id
        self.insight_id = insight_id
        self.insight_folder = insight_folder
        
    def addDocument(
        self,
        file_paths:List[str],
        engine_id:Optional[str] = None, 
        insight_id:Optional[str] = None,
        param_dict:Optional[Dict] = {}
    ) -> bool:
        engine_id, insight_id = self._determine_ids(
            engine_id = engine_id,
            insight_id = insight_id
        )
        
        param_dict['insight'] = insight_id
        
        # get the file paths
        file_paths = self.get_files(file_paths=file_paths)

        epoc = super().get_next_epoc()
        super().call(
            epoc = epoc, 
            engine_type = VectorEngine.engine_type, 
            engine_id = engine_id, 
            method_name = 'addDocument', 
            method_args=[file_paths, param_dict],
            method_arg_types=['java.util.List', 'java.util.Map'],
            insight_id = insight_id
        )
        
        return True
            
    def removeDocument(
        self,
        file_names:List[str],
        engine_id:Optional[str] = None, 
        insight_id:Optional[str] = None,
        param_dict:Optional[Dict] = {}
    ) -> bool:
        engine_id, insight_id = self._determine_ids(
            engine_id = engine_id,
            insight_id = insight_id
        )
        
        param_dict['insight'] = insight_id
        
        epoc = super().get_next_epoc()
        super().call(
            epoc = epoc, 
            engine_type = VectorEngine.engine_type, 
            engine_id = engine_id, 
            method_name = 'removeDocument', 
            method_args=[file_names, param_dict],
            method_arg_types=['java.util.List', 'java.util.Map'],
            insight_id = insight_id
        )[0]
        
        return True
    
    def nearestNeighbor(
        self,
        search_statement:str,
        limit:Optional[int] = 5,
        param_dict:Optional[Dict] = {},
        engine_id:Optional[str] = None, 
        insight_id:Optional[str] = None,
    ) -> List[Dict]:
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
        engine_id:Optional[str] = None, 
        insight_id:Optional[str] = None,
        param_dict:Optional[Dict]= {}
    ) -> List[Dict]:
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
    
    def get_files(self, file_paths: List[str]) -> List[str]:
        valid_files = []

        for file_path in file_paths:
            # Update file_path to include the insight folder path
            if not os.path.isfile(file_path):
                updated_file_path = os.path.join(self.insight_folder, file_path)
                if os.path.isfile(updated_file_path):
                    file_path = updated_file_path
                else:
                    raise IOError(f'Unable to find file path for {file_path}')

            if self._is_zip_file(file_path):
                valid_files_in_zip = self._unzip_and_filter(file_path, os.path.splitext(file_path)[0])
                valid_files.extend(valid_files_in_zip)
            elif self._is_supported_file_type(file_path):
                valid_files.append(file_path)

        return valid_files
            
    
    @staticmethod
    def _unzip_and_filter(zip_file_path, dest_directory) -> List[str]:
        valid_file_paths = []

        def extract_file(z, entry, file_path):
            dir_path = os.path.dirname(file_path)
            os.makedirs(dir_path, exist_ok=True)
            with open(file_path, 'wb') as f:
                shutil.copyfileobj(z.open(entry), f)

        with zipfile.ZipFile(zip_file_path, 'r') as z:
            for entry in z.namelist():
                file_path = os.path.join(dest_directory, entry)
                if not entry.endswith('/') and VectorEngine._is_supported_file_type(file_path):
                    extract_file(z, entry, file_path)
                    valid_file_paths.append(file_path)
                elif entry.endswith('/'):
                    os.makedirs(file_path, exist_ok=True)
                elif VectorEngine._is_zip_file(file_path):
                    extract_file(z, entry, file_path)
                    parent_path = os.path.dirname(file_path)
                    base_name = os.path.splitext(os.path.basename(file_path))[0]
                    nested_dest_directory = os.path.join(parent_path, base_name)
                    nested_valid_paths = VectorEngine._unzip_and_filter(file_path, nested_dest_directory)
                    valid_file_paths.extend(nested_valid_paths)

        return valid_file_paths

    @staticmethod
    def _is_supported_file_type(file_path):
        return file_path.split('.')[-1].lower() in {'pdf', 'pptx', 'ppt', 'doc', 'docx', 'txt', 'csv'}

    @staticmethod
    def _is_zip_file(file_path):
        return file_path.split('.')[-1].lower() == 'zip'