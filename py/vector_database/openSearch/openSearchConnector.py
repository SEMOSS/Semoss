import pandas as pd
from opensearchpy import OpenSearch
from opensearchpy import helpers
from typing import Any, Dict, List, Mapping, Optional, Union
import gaas_gpt_model as ggm
from gaas_gpt_model import ModelEngine
from sentence_transformers import SentenceTransformer
import numpy as np

"""The below is needed to test locally for now, will remove in final patch"""
# model_name = "all-MiniLM-L6-v2"
# model = SentenceTransformer(model_name)

Hosts = Union[str, List[Union[str, Mapping[str, Union[str, int]]]]]

class OpenSearchConnection():
    '''
    This is the primary class to connect to an Open Search Database  
    '''

    def __init__(
            self, 
            index_name: str,
            username: str,
            password: str,
            distance_method: str,
            embedder_engine_id: str,
            indexList: list = [str],
            hosts: Optional[Hosts] = None,
            embedder_engine: ModelEngine = None,
           **kwargs,
    )-> None:
        '''
        Create an instance of a OpenSearchConnetor 
        TODO Want to double check if we should have certs on
        '''
        self.hosts = hosts
        self.client = OpenSearch(hosts, http_auth=(username, password), verify_certs=False)
        self.index_name = index_name
         # Check client connection, this will raise if not connected
        self.client.info()
        # set the embedder class so it can be used when new searchers/indexClasses are added
        if embedder_engine is not None:
            self.embeddings_engine = embedder_engine
        else:
            self.embeddings_engine = ModelEngine(engine_id = embedder_engine_id) 

         # configure mapping for the embedding field
        embedding_dim = kwargs.get("embedding_dim", 768)
        method = kwargs.get("method", None)


        '''
        This is the mapping when adding in a document, leaving it at class level for now
        It is also needed for connecting to a specific index
        '''
        mappings: Dict[str, Any] = {
            "properties": {
                "embedding": {"type": "knn_vector", "index": True, "dimension": embedding_dim},
                "content": {"type": "text"},
            },
            "dynamic_templates": [
                {
                    "strings": {
                        "path_match": "*",
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "keyword",
                        },
                    }
                }
            ],
        }

        if method:
            mappings["properties"]["embedding"]["method"] = method

        mappings = kwargs.get("mappings", mappings)
        settings = kwargs.get("settings", {"index.knn": True})

        body = {"mappings": mappings, "settings": settings}

        '''TODO this will be needed for the patch when adding or creating an index'''
        if not self.client.indices.exists(index=index_name):
            self.client.indices.create(index=index_name, body=body)


    def getClient(self)-> OpenSearch:
        '''
        Return the client
        '''
        return self.client
    
    def count_documents(self) -> int: 
        '''
        Returns the number of documents in the index
        '''
        return self.getClient().count(index=self.index_name)["count"]
    
    # def add_document(self, documents: List[]) -> int:
    #     '''
    #     Adds in documents to opensearch 
    #     TODO will be added in a later date 
    #     '''

    def knn_search(
            self, 
            question: str,
            insight_id: str,
            fields: Optional[str] = None,
            limit: Optional[int] = 3, 
            columns_to_return: Optional[List[str]] = None, 
        ) -> List[Dict]:
        """
        Given a index (set of vector databases), find the closest matches (default to 3) using the knn query of a opensearch database

        Args:
            question:
                the question that will be embedded and queried against 
            insight_id:
                insight id, needed to get access to the ModelEngine embed fucntion 
            fields(Optional ): 
                To query for speific fields, updated in the query mapping. Targets specific fields or columns. 
                Future enhancement
        Return:
            List[Dict] consisting of Score and ids 

        Example: 
            Running >>>aSouZbW.knn_search(question="/Movies about spies/", insight_id='8d0e9393-b04a-4516-8c6c-657739048df2', limit='3')
        TODO update on how we want to return the data, won't have access to certain information that other VDs return. 
        """
        if not question:
            msg = "query must be a non empty string"
            raise ValueError(msg)
        
        # if columns_to_return is None, then by default we return all columns
        # if(columns_to_return is None):
        #     columns_to_return = list(self.ds.features)


        search_vector = self.embeddings_engine.embeddings(
            strings_to_embed = [question], 
            insight_id = insight_id
        )
        query_vector = np.array(search_vector[0]['response'], dtype=np.float32)
        query_vector = query_vector[0]
        
        # Needed for local testing to test open search database 
        # search_vector = model.encode(question)

        body: Dict[str, Any] = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "knn": {
                                "embedding": {
                                    "vector": search_vector,
                                    "k": limit,
                                }
                            }
                        }
                    ],
                }
            },
            "size": limit,
        }
        
        if fields:
            body["query"]["bool"]["fields"] = fields

        '''
        TODO Future Enchancement:
            Update to check against list of index. And then confifutre to return the highest scores. 
        First need to check if that is how we want to use the connection ability. 
        '''
        res = self.getClient().search(index=self.index_name, body=body)
        final_output: List[Dict] = [self._createDictFromReturn(hit) for hit in res["hits"]["hits"]]
        return final_output
    
    def _createDictFromReturn(self, hit: Dict[str, Any]) -> Dict:
        """
        Helper method to create a Dict from the search hit provided.
        """
        score = hit["_score"]
        sourceData = hit["_source"]
        sourceData.pop('embedding', None)
        output = {}
        output.update({'Score' :score})
        output.update({'Source': sourceData})
        return output
