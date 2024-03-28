### ATTENTION 
### To run this you need to be in the Semoss_Dev/py directory
### Then run the following command
### py -3.10 -m unittest vector_database.tests.test_faiss_vector_db

import unittest
import os
from typing import Dict

from genai_client import get_tokenizer
import vector_database


class FaissVectorDatabaseTests(unittest.TestCase):
    
    test_db = "test_vector"
    test_files_path = None
    
    @classmethod
    def setUpClass(cls):
        if cls.test_files_path is None:
            cls.test_files_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_files")
            
    def test_nearestNeighbor_with_local_model(self):
        # declare the model
        cfg_tokenizer = get_tokenizer(
            tokenizer_name = 'BAAI/bge-large-en-v1.5', 
            max_tokens = None, 
            tokenizer_type = 'EMBEDDED'
        )
        
        
        # create a local model engine by passing the engine id
        # to do this you might need to pass in your semoss_dev path
        # Defaults are windows: C:/workspace/Semoss_Dev and non-windows: /opt/semosshome
        embedder_engine = ModelEngine(
            engine_id = 'cb661f04-cb30-48fd-aafc-b4d422ce24e4',
            model_engine_class = "LOCAL"
        )

        # # create a local model engine by passing the smss path
        # embedder_engine = ModelEngine(
        #     engine_smss_file_path = 'C:/workspace/Semoss_Dev/model/GPT3-Turbo__2c6de0ff-62e0-4dd0-8380-782ac4d40245.smss',
        #     local=True
        # )

        # # ceate a local model engine by creating the model directly from genai_client and then passing it in
        # from genai_client import TextGenClient
        # wizardModel = TextGenClient(
        #     endpoint="<enter_model_endpoint>",
        #     model_name="garage-bAInd/Platypus2-70B-instruct",
        #     max_tokens=8192,
        #     max_input_tokens=4096,
        # )

        # from gaas_gpt_model import ModelEngine
        # embedder_engine = ModelEngine(
        #     model_engine = wizardModel,
        #     local=True
        # )
        
        from gaas_gpt_model import ModelEngine
        aGnETVb = vector_database.FAISSDatabase(
            embedder_engine=embedder_engine, 
            tokenizer=cfg_tokenizer, 
            distance_method='Squared Euclidean (L2) distance'
        )

        path_to_index_class = os.path.join(self.test_files_path, self.test_db, "schema/default")
        
        aGnETVb.create_searcher(searcher_name = 'default', base_path = path_to_index_class)
        aGnETVb.searchers['default'].load_dataset(path_to_index_class + '/dataset.pkl')
        aGnETVb.searchers['default'].load_encoded_vectors(path_to_index_class + '/vectors.pkl')
        search_results = aGnETVb.searchers['default'].nearestNeighbor(question='how is the president chosen?')
        
        self.assertIsInstance(search_results, list)
        
        for row in search_results:
            self.assertIsInstance(row, dict)
        
    def test_nearestNeighbor_with_tomcat_via_ai_server(self):
        '''
        NEED TO HAVE TOMCAT RUNNING
        '''
        
        cfg_tokenizer = get_tokenizer(
            tokenizer_name = 'BAAI/bge-large-en-v1.5', 
            max_tokens = None, 
            tokenizer_type = 'EMBEDDED'
        )
        
        from ai_server import ServerClient, ModelEngine
        server_client = ServerClient(
            access_key="***REMOVED***",             
            secret_key="***REMOVED***",             
            base="http://localhost:9090/Monolith_Dev/api"
        )
        
        aGnETVb = vector_database.FAISSDatabase(
            embedder_engine_id='cb661f04-cb30-48fd-aafc-b4d422ce24e4',
            model_engine_class=ModelEngine,
            tokenizer=cfg_tokenizer, 
            distance_method='Squared Euclidean (L2) distance'
        )

        path_to_index_class = os.path.join(self.test_files_path, self.test_db, "schema/default")
        
        aGnETVb.create_searcher(searcher_name = 'default', base_path = path_to_index_class)
        aGnETVb.searchers['default'].load_dataset(path_to_index_class + '/dataset.pkl')
        aGnETVb.searchers['default'].load_encoded_vectors(path_to_index_class + '/vectors.pkl')
        search_results = aGnETVb.searchers['default'].nearestNeighbor(question='how is the president chosen?')
        
        self.assertIsInstance(search_results, list)
        
        for row in search_results:
            self.assertIsInstance(row, dict)