from typing import Optional, Union, List, Dict, Any
from sentence_transformers import SentenceTransformer, util
from huggingface_hub import try_to_load_from_cache, _CACHED_NO_EXIST, snapshot_download, hf_hub_download
from transformers import AutoTokenizer
from pathlib import Path

class LocalEmbedder():

    def __init__(
        self,
        model_path:str = None
    ) -> None:
        self.model_name = model_path
        self.model_folder = self.get_physical_folder(repo_id = model_path)
        self.encoder = SentenceTransformer(
            self.model_folder
        )
        self.max_tokens = self.encoder.max_seq_length
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
    
    def get_physical_folder(
        self, 
        repo_id:str
    ) -> str:
        filepath = try_to_load_from_cache(
            repo_id=repo_id, 
            filename='config.json'
        )

        if isinstance(filepath, str):
            # file exists and is cached
            return Path(filepath).parent.absolute()
        # elif filepath is _CACHED_NO_EXIST:
        #     # hopefully we are just missing the config file
        #     config_file = hf_hub_download(
        #         repo_id= repo_id,
        #         filename='config.json'
        #     )
        #     return Path(config_file).parent.absolute()
        else:
            try:
                # file does not exist so we need to download the repo
                return snapshot_download(repo_id)
            except:
                # really dont want to have to do this
                from transformers import AutoModel
                AutoModel.from_pretrained(repo_id)
                return try_to_load_from_cache(
                    repo_id=repo_id, 
                    filename='config.json'
                )

    def embeddings(
        self, 
        list_to_embed:List[str], 
        prefix=""
    ) -> List[float]:
        # Determine what object was bassed in so we can pre-configure it before making the call
        assert isinstance(list_to_embed, list) or isinstance(object_to_encode, str)
        

        embedded_tensor = self.encoder.encode(
            sentences = list_to_embed, 
            show_progress_bar=True
        )
        
        total_tokens = sum([len(self.tokenizer.encode(chunk)) for chunk in list_to_embed])

        # TODO find a way to push back batches like OpenAI
        # THIS IS SLOW AS HECK
        # embedded_list = []
        # number_of_items_to_encode = len(list_to_embed)
        # for i in range(number_of_items_to_encode):
        #   embedded_list.append(
        #     self.encoder.encode(list_to_embed[i])
        #   )
        #   print(prefix + "Completed Embedding " + str(i) + "/" + str(number_of_items_to_encode) + " Chunks")
        
        return {
            'response':embedded_tensor.tolist(),
            'numberOfTokensInPrompt': total_tokens,
            'numberOfTokensInResponse': 0
        }

    def ask(
        self, 
        *args: Any, 
        **kwargs
    ) -> str:
        response = 'This model does not support text generation.'
        output_payload = {
            'response':response,
            'numberOfTokensInPrompt': 0,
            'numberOfTokensInResponse': len(self.tokenizer.encode(response))
        }
        return output_payload