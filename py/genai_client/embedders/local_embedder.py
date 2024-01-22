from typing import Optional, Union, List, Dict, Any
from sentence_transformers import SentenceTransformer, util
from huggingface_hub import try_to_load_from_cache, _CACHED_NO_EXIST, snapshot_download, hf_hub_download
from transformers import AutoModel
from pathlib import Path
from ..tokenizers.huggingface_tokenizer import HuggingfaceTokenizer
from ..constants import (
    MAX_TOKENS,
    MAX_INPUT_TOKENS,
    ModelEngineResponse
)


class LocalEmbedder():

    def __init__(
        self,
        model_name:str,
        model_path: Optional[str] = None,
        **kwargs
    ) -> None:
        # TODO - remove or options once existing local embedders have been changed
        self.model_name = model_name
        
        self.model_folder = model_path or self.get_physical_folder(repo_id = self.model_name)
        
        assert self.model_folder != None
        
        self.embedder = self.get_embedder()

        self.tokenizer = HuggingfaceTokenizer(
            encoder_name = self.model_name, 
            max_tokens = kwargs.pop(
                MAX_TOKENS, 
                None
            ),
            max_input_tokens = kwargs.pop(
                MAX_INPUT_TOKENS, 
                None
            )
        )
        
        self.key_bert_model = None
    
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
                AutoModel.from_pretrained(repo_id)
                return try_to_load_from_cache(
                    repo_id=repo_id, 
                    filename='config.json'
                )
                
    def get_embedder(
        self
    ):
        embedder = None
        try:
            embedder = SentenceTransformer(
                self.model_folder
            )
        except:
            # trust_remote_code is needed to use the encode method
            embedder = AutoModel.from_pretrained(
                self.model_folder, 
                trust_remote_code=True
            )

        return embedder

    def embeddings(
        self, 
        list_to_embed:List[str], 
        prefix=""
    ) -> List[float]:
        
        # Determine what object was bassed in so we can pre-configure it before making the call
        assert isinstance(list_to_embed, list)


        embedded_list = self.embedder.encode(
            sentences = list_to_embed, 
        )
        
        total_tokens = sum([self.tokenizer.count_tokens(chunk) for chunk in list_to_embed])

        # TODO find a way to push back batches like OpenAI
        # THIS IS SLOW AS HECK
        # embedded_list = []
        # number_of_items_to_encode = len(list_to_embed)
        # for i in range(number_of_items_to_encode):
        #   embedded_list.append(
        #     self.embedder.encode(list_to_embed[i])
        #   )
        #   print(prefix + "Completed Embedding " + str(i) + "/" + str(number_of_items_to_encode) + " Chunks")
        
        if not isinstance(embedded_list, list):
            embedded_list = embedded_list.tolist()
        
        model_engine_response = ModelEngineResponse(
            response=embedded_list,
            prompt_tokens=total_tokens,
            response_tokens=0
        )
        
        return model_engine_response.to_dict()

    def ask(
        self, 
        *args: Any, 
        **kwargs
    ) -> str:
        response = 'This model does not support text generation.'
        model_engine_response = ModelEngineResponse(
            response=response,
            prompt_tokens=0,
            response_tokens=self.tokenizer.count_tokens(response)
        )
        
        return model_engine_response.to_dict()
    
    def model(
        self,
        input: List[str],
        percentile:int = 0,
        max_keywords:int = 12
    ) -> List[str]:
        
        if not isinstance(percentile, int):
            percentile = int(percentile)
        
        if not isinstance(max_keywords, int):
            max_keywords = int(max_keywords)
        
        # define what the actual input object is
        list_of_chunks = input
        
        kw_model= self.get_key_bert_model()
        keywords = LocalEmbedder.get_text_keywords(
                kw_model=kw_model, 
                list_of_chunks=list_of_chunks, 
                percentile=percentile,
                max_keywords=max_keywords
            )
        
        return keywords
    
    def get_key_bert_model(
        self
    ):
        if self.key_bert_model == None:
            from keybert import KeyBERT
            # from keyphrase_vectorizers impoirt KeyphraseCountVectorizer
            from transformers import PreTrainedModel
            
            # if (key_bert_model_name != None):
            #     self.key_bert_model = KeyBERT(model=key_bert_model_name)
            if (isinstance(self.embedder, PreTrainedModel)):
                from transformers import pipeline
                self.key_bert_model = KeyBERT(
                    model = pipeline(
                        task= 'feature-extraction',
                        model=self.embedder, 
                        tokenizer=self.tokenizer.tokenizer
                    )
                )
            elif (isinstance(self.embedder, SentenceTransformer)):
                from keybert.backend._sentencetransformers import SentenceTransformerBackend
                self.key_bert_model = KeyBERT(
                    model = SentenceTransformerBackend(
                        embedding_model = self.embedder
                    )
                )
            else:
                raise TypeError("Only SentenceTransformer and PreTrainedModel embedders support the model method.")
            
        return self.key_bert_model  
        
    def get_text_keywords(
        kw_model, 
        list_of_chunks:List[str], 
        percentile:int, 
        max_keywords:int
    ) -> List[str]:
        '''Extracts keyword from the text string
            Args:
                kw_model (`KeyBERT`) : the keybert model being used to find the keywords
                text (`str`): a text string to extract keywords from
                perc (`int`): percentile threshold for the keywords to extract
                max_keywords (`int`): maxmimum numbers of keywords to return
        
            Returns
                concatendated keywords (`List[str]`) - dataframe with extracted keywords and their probabilities
        '''

        # import enchant
        import numpy as np
        from keyphrase_vectorizers import KeyphraseCountVectorizer

        # define the english dictionary
        # language_dictionary = enchant.Dict("en_US") 
        
        if len(list_of_chunks) == 1:
            master_keywords_list = [kw_model.extract_keywords(
                list_of_chunks,
                top_n=max_keywords,
                vectorizer=KeyphraseCountVectorizer(),
                use_mmr=True
            )]
        else:
            master_keywords_list = kw_model.extract_keywords(
                list_of_chunks,
                top_n=max_keywords,
                vectorizer=KeyphraseCountVectorizer(),
                use_mmr=True
            )
                
        # capitilize the outputs since the dictionary check doesnt seem valid with lower case
        # TODO check other option -- embeddings do seem to come back the same
        # master_keywords_list = [[(word.upper(), value) for word, value in inner_list] for inner_list in master_keywords_list]
        
        for i, keywords in enumerate(master_keywords_list):
            
            if len(keywords) > 0:
                # temp_keywords = [item for item in keywords if language_dictionary.check(item[0])]
                # if len(temp_keywords) == 0:
                #     # we need to make sure that the list isnt empty after validating the words
                #     keywords = [('',1.0)]
                # else:
                #     keywords = temp_keywords
                keywords = keywords
            else:
                keywords=[('',1.0)]

            
            prob = [item[1] for item in keywords]
            
            # get the threshold based on percentile
            threshold = np.percentile(
                prob,
                percentile
            )

            filtered_data = [word for word, score in keywords if score >= threshold]
            
            master_keywords_list[i] = ' '.join(filtered_data)

        
        return master_keywords_list
