from typing import (
    List, 
    Optional, 
    Dict,
    Union,
    Any
)

from abc import (
    ABC,
    abstractmethod
)

import os

from gaas_server_proxy import ServerProxy


class AbstractModelEngine(ABC):
    '''This is an abstract class the defined what methods need to be implemeted for a ModelEngine'''
    
    @abstractmethod
    def ask(self, *args: Any, **kwargs: Any) -> List[Dict]:
        '''This method is responsible for interacting with models that can perform text-generation'''
        pass
    
    @abstractmethod
    def embeddings(self, *args: Any, **kwargs: Any) -> List[Dict]:
        '''This method is responsible for interacting with models that can create embeddings from strings'''
        pass
    
    @abstractmethod
    def model(self, *args: Any, **kwargs: Any) -> List[Any]:
        '''This method is responsible for utilizing a specific model function that is unique to that model function'''
        pass

    @abstractmethod
    def do_call(self, method_name:str, input: Any, **kwargs: Any) -> Any:
        '''This method is responsible for utilizing a specific tokenize function that is unique to that tokenize function'''
        pass
    
    @abstractmethod
    def get_model_engine_id(self) -> str:
        '''This method returns the model engine id of the `AbstractModelEngine` class. If the engine has not been set then it returns `None`.'''
        pass

    
class TomcatModelEngine(AbstractModelEngine, ServerProxy):
    '''This class implements AbstractModelEngine class and is used as the "ModelEngine" class when calling `from gaas_gpt_model import ModelEngine` from a python 
    process in Tomcat Server'''

    def __init__(
        self, 
        engine_id: str, 
        insight_id: Optional[str] = None,
        **kwargs
    ):
        '''
        Initialize the TomcatModelEngine instance.

        Args:
            engine_id (`str`): Identifier of the model engine.
            insight_id (`Optional[str]`): Identifier for insights.
            local (`Optional[bool]`): Whether the model runs locally.
            pipeline_type (`Optional[str]`): Type of pipeline for local models.
            **kwargs: Additional keyword arguments.
        '''
        super().__init__()                  # initialize the ServerProxy class
        self.engine_id = engine_id          # set the engine id
        self.insight_id = insight_id        # set the insight id
            
    def ask(
        self, 
        question: str, 
        context: Optional[str] = None, 
        param_dict: Optional[Dict] = None,
        insight_id: Optional[str] = None
    ) -> List[Dict]:
        '''This method is responsible for interacting with models that can perform text-generation
        
        Args:
            - question (str): The question to ask.
            - context (Optional[str]): Context for the question.
            - insight_id (Optional[str]): Identifier for insights.
            - param_dict (Optional[Dict]): Additional parameters.
            
        Returns:
            `List[Dict]`: A dictionary with the response from the text-generation model. The dictionary in the response will contain the following keys:
            - response
            - numberOfTokensInPrompt
            - numberOfTokensInResponse
            - messageId
            - roomId
        '''
        
        if insight_id is None:
            insight_id = self.insight_id
            
        epoc = super().get_next_epoc()
        
        model_response = super().call(
            epoc=epoc, 
            engine_type='Model', 
            engine_id=self.engine_id, 
            method_name='ask', 
            method_args=[question,context, insight_id, param_dict],
            method_arg_types=['java.lang.String', 'java.lang.String', 'prerna.om.Insight', 'java.util.Map'],
            insight_id=insight_id
        )
        
        return model_response
  
    def embeddings(
        self, 
        strings_to_embed: List[str], 
        param_dict: Optional[Dict] = None,
        insight_id: Optional[str] = None
    ) -> List[Dict]:
        if insight_id is None:
            insight_id = self.insight_id
            
        if isinstance(strings_to_embed,str):
            strings_to_embed = [strings_to_embed]
              
        assert isinstance(strings_to_embed, list)

        epoc = super().get_next_epoc()
        model_response = super().call(
            epoc=epoc, 
            engine_type='Model', 
            engine_id=self.engine_id, 
            method_name='embeddings', 
            method_args=[strings_to_embed, insight_id, param_dict],
            method_arg_types=['java.util.List', 'prerna.om.Insight', 'java.util.Map'],
            insight_id=insight_id
        )
        
        return model_response
    
    def model(
        self,
        input: Any,
        param_dict: Optional[Dict] = None,
        insight_id: Optional[str] = None, 
    ):
        if insight_id is None:
            insight_id = self.insight_id
                         
        epoc = super().get_next_epoc()
        model_response = super().call(
            epoc=epoc, 
            engine_type='Model', 
            engine_id=self.engine_id, 
            method_name='model', 
            method_args=[input, insight_id, param_dict],
            method_arg_types=['java.lang.Object', 'prerna.om.Insight', 'java.util.Map'],
            insight_id=insight_id, 
        )
        
        return model_response

    def get_model_type(
        self, 
        insight_id: Optional[str] = None
    ):
        if insight_id is None:
            insight_id = self.insight_id
            
        epoc = super().get_next_epoc()
        return super().call(
            epoc=epoc, 
            engine_type='Model', 
            engine_id=self.engine_id, 
            method_name='getModelType', 
            method_args=[],
            method_arg_types=[],
            insight_id=insight_id,
        )
          
    # this is a little bit of get out of jail free card
    def do_call(
        self,
        method_name: str,
        input: Any,
        **kwargs
    ) -> Any:
        call_maker = getattr(self, method_name, None) 
        if call_maker is not None:
            return call_maker(input, **kwargs)
        else:
            return None
        
    def get_model_engine_id(self) -> str:
        return self.engine_id
        
class HuggingFacePipelineModelEngine(AbstractModelEngine):
    '''This class implements AbstractModelEngine class and is used as the "ModelEngine" class when calling `from gaas_gpt_model import ModelEngine` from a python 
    process in Tomcat Server'''

    def __init__(
        self, 
        engine_id: str, 
        pipeline_type: Optional[str] = None,
        **kwargs
    ):
        '''
        Initialize the TomcatModelEngine instance.

        Args:
            engine_id (`str`): Identifier of the model engine.
            insight_id (`Optional[str]`): Identifier for insights.
            local (`Optional[bool]`): Whether the model runs locally.
            pipeline_type (`Optional[str]`): Type of pipeline for local models.
            **kwargs: Additional keyword arguments.
        '''
        super().__init__()                  # initialize the ServerProxy class
        self.engine_id = engine_id          # set the engine id
        
        # start the model and make it available
        import torch
        from transformers import pipeline 
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        self.pipeline_type = pipeline_type
        self.pipe = pipeline(pipeline_type, model=engine_id, device=device)
    
    def ask(
        self, 
        question: str, 
        context: Optional[str] = None, 
        param_dict: Optional[Dict] = None
    ) -> List[Dict]:
        '''This method is responsible for interacting with models that can perform text-generation
        
        Args:
            - question (str): The question to ask.
            - context (Optional[str]): Context for the question.
            - insight_id (Optional[str]): Identifier for insights.
            - param_dict (Optional[Dict]): Additional parameters.
            
        Returns:
            `List[Dict]`: A dictionary with the response from the text-generation model. The dictionary in the response will contain the following keys:
                - response
                - numberOfTokensInPrompt
                - numberOfTokensInResponse
                - messageId
                - roomId
        '''       
        raise NotImplementedError("HuggingFacePipelineModelEngine does not have an ask method implemented")
  
    def embeddings(
        self, 
        strings_to_embed: List[str], 
        param_dict: Optional[Dict] = None
    ) -> List[Dict]:
        return self.pipe.model.encode(strings_to_embed)
    
    def model(
        self,
        input: Any,
        param_dict: Optional[Dict] = None,
    ):
        return self.pipe(input)

    def get_model_type(
        self, 
    ):
        return self.pipeline_type
          
    # this is a little bit of get out of jail free card
    def do_call(
        self,
        method_name: str,
        input: Any,
        **kwargs
    ) -> Any:
        call_maker = getattr(self, method_name, None) 
        if call_maker is not None:
            return call_maker(input, **kwargs)
        else:
            return None
        
    def get_model_engine_id(self) -> str:
        return self.engine_id
    
    
class LocalModelEngine(AbstractModelEngine):
  
    def __init__(
        self,
        model_engine: Any = None, 
        engine_id: Optional[str] = None,
        engine_smss_file_path: Optional[str] = None,
        semoss_dev_path: Optional[str] = 'C:/workspace/Semoss_Dev' if os.name == 'nt' else '/opt/semosshome',
    ):

        # determine how to create the model engine locally
        if engine_smss_file_path is not None or engine_id is not None:
            
            if engine_smss_file_path is not None:
                # the direct path of the smss file was passed in
                pass
            elif engine_id is not None:
                # the gave the engine id so try to find the smss file
                engine_smss_file_path = LocalModelEngine.get_model_smss_file(
                    semoss_dev_path, 
                    engine_id
                )
            
            # get the smss props from the smss file
            smss_props = LocalModelEngine.read_smss_file(engine_smss_file_path)
            
            # use the smss props to initialize the model engine
            model_engine_init_command = LocalModelEngine.get_init_model_commads(smss_props)
            exec(model_engine_init_command)
            
            self.local_model_engine = locals().get(smss_props['VAR_NAME'], None)
            self.engine_id = smss_props.get('ENGINE', None)
        else:
            self.local_model_engine = model_engine
            self.engine_id = None
            
        assert self.local_model_engine != None, "Unable to define a Local Model Engine based on the parameters passed in"
    
    def ask(
        self, 
        **kwargs
    ) -> Dict:
        return [self.local_model_engine.ask(**kwargs)]
  
    def embeddings(
        self, 
        **kwargs
    ) -> Dict:
        return [self.local_model_engine.embeddings(**kwargs)]
    
    def model(
        self,
        **kwargs
    ):
        return [self.local_model_engine.model(**kwargs)]

    def get_model_type(
        self,
        **kwargs
    ):
        #TODO add model type in python as well
        return ['TEXT_GENERATION']
        
    def do_call(
        self,
        method_name: str,
        input: Any,
        **kwargs
    ) -> Any:
        call_maker = getattr(self, method_name, None) 
        if call_maker is not None:
            return call_maker(input, **kwargs)
        else:
            return None
        
    def get_model_engine_id(self) -> str:
        return self.engine_id
        
    
    @staticmethod
    def get_model_smss_file(semoss_dev_file_path:str, engine_id:str) -> str:
        '''This method returns a list of smss files in the semosshome model directory'''
        import glob
        
        file_pattern = '*.smss'

        # Use glob.glob to find all matching files in the directory
        model_smss_files = glob.glob(
            os.path.join(semoss_dev_file_path, 'model', file_pattern)
        )
        model_smss_files.sort()
        
        for smss_file_name in model_smss_files:
            if smss_file_name.find(engine_id) > 0:
                return smss_file_name
            
        raise ValueError(f'Unable to find smss file for the engine id: {engine_id}')

        
    @staticmethod
    def read_smss_file(file_path:str) -> Dict:
        smss_props = {}
        with open(file_path.replace('\\','/'), 'r') as file:
            for line in file:
                line = line.strip()
                if line.startswith('#') or len(line) == 0:
                    continue
                try:
                    key, value = line.split(None, 1)  # Split by any whitespace
                except:
                    pass
                smss_props[key] = True if value.lower() == 'true' else (False if value.lower() == 'false' else value.replace('\\','/'))
        return smss_props

    @staticmethod
    def get_init_model_commads(smss_props:dict) -> str:
        import re

        init_model_engine_template = smss_props['INIT_MODEL_ENGINE']

        # Find all placeholders in the template string
        placeholders = re.findall(r'\${(.*?)}', init_model_engine_template)

        # Create a dictionary with the actual values for the found placeholders
        values = {placeholder: smss_props[placeholder] for placeholder in placeholders if placeholder in smss_props}

        # Substitute the placeholders with the actual values from the dictionary
        formatted_string_dynamic = init_model_engine_template
        for placeholder, value in values.items():
            formatted_string_dynamic = formatted_string_dynamic.replace('${' + placeholder + '}', value)

        return formatted_string_dynamic
    
class ModelEngine(AbstractModelEngine):
    
    def __init__(
        self,
        model_engine_class: Optional[str] = "TOMCAT",
        **kwargs
    ):
        if model_engine_class == "TOMCAT":
            self.model_engine = TomcatModelEngine(**kwargs)
        elif model_engine_class == "LOCAL":
            self.model_engine = LocalModelEngine(**kwargs)
        elif model_engine_class == "HF_PIPELINE":
            pass
        else:
            raise ValueError("Unable to define a Model Engine. Model Engine Class types are 'TOMCAT', 'LOCAL', or 'HF_PIPELINE'.")
        
    def ask(
        self,
        insight_id: Optional[str] = None,               # TODO remove once users stop using it. No longer needs to be set.
        **kwargs
    ) -> Dict:
        return self.model_engine.ask(**kwargs)
  
    def embeddings(
        self,
        insight_id: Optional[str] = None,               # TODO remove once users stop using it. No longer needs to be set.
        **kwargs
    ) -> Dict:
        return self.model_engine.embeddings(**kwargs)
    
    def model(
        self,
        insight_id: Optional[str] = None,               # TODO remove once users stop using it. No longer needs to be set.
        **kwargs
    ):
        return self.model_engine.model(**kwargs)

    def get_model_type(
        self, 
        **kwargs
    ):
        return self.model_engine.get_model_type(**kwargs)
     
    def do_call(
        self, 
        **kwargs: Any
    ) -> Any:
        return self.model_engine.embeddings(**kwargs)
    
    def get_model_engine_id(self) -> str:
        return self.model_engine.get_model_engine_id()
    
    def to_langchain_embedder(self):
        '''Transform the model engine into a langchain `Embeddings`object so that it can be used with langchain code'''
       
        from langchain_core.embeddings import Embeddings
        class CfgEmbeddingsEngine(Embeddings):
            def __init__(self, modelEngine):
                self.modelEngine = modelEngine

            def embed_documents(self, texts: List[str]) -> List[List[float]]:
                """Embed search docs."""
                return self.modelEngine.embeddings(strings_to_embed=texts)[0]["response"]

            def embed_query(self, text: str) -> List[float]:
                return self.modelEngine.embeddings(strings_to_embed=[text])[0]["response"][0]
            
        return CfgEmbeddingsEngine(modelEngine=self)
    
    def to_langchain_chat_model(self):
        '''Transform the model engine into a langchain `BaseChatModel` object so that it can be used with langchain code'''
        from langchain_core.language_models.chat_models import BaseChatModel
        from langchain_core.outputs import (
            ChatGeneration,
            ChatResult,
        )
        from langchain_core.messages import (
            AIMessage,
            BaseMessage,
        )

        class ChatCfgAI(BaseChatModel):
            engine_id: str
            model_engine: ModelEngine
            model_type: str

            def __init__(self, model_engine):
                data = {
                    "engine_id": model_engine.get_model_engine_id(),
                    "model_engine": model_engine,
                    "model_type": model_engine.get_model_type()[0],
                }

                super().__init__(**data)

            class Config:
                """Configuration for this pydantic object."""

                allow_population_by_field_name = True

            def _generate(
                self,
                messages: List[BaseMessage],
                stop: Optional[List[str]] = None,
                **kwargs: Any,
            ) -> ChatResult:
                """Top Level call"""
                full_prompt = self.convert_messages_to_full_prompt(messages)
                response = self.model_engine.ask(
                    question="", param_dict={**kwargs, **{"full_prompt": full_prompt}}
                )

                return self._create_chat_result(response=response[0])

            def _create_chat_result(self, response: Dict[str, Any]) -> ChatResult:
                generations = []

                message = response.pop("response", "")
                generation_info = dict()
                if "logprobs" in response.keys():
                    generation_info["logprobs"] = response.pop("logprobs", {})
                gen = ChatGeneration(
                    message=AIMessage(content=message),
                    generation_info=generation_info,
                )

                generations.append(gen)

                return ChatResult(generations=generations, llm_output=response)

            def convert_messages_to_full_prompt(
                self,
                messages: List[BaseMessage],
            ) -> Union[Dict[str, Any], str]:
                """Convert a LangChain message to a the correct response for a model.

                Args:
                    message: The LangChain message.

                Returns:
                    The `Dict` or `str` containing the message payload.
                """

                if self.model_type in ["OPEN_AI", "VERTEX"]:
                    # assume this is a chat based openai model, otherwise why would you call this
                    # class
                    full_prompt: List[Dict[str, Any]]
                    from langchain_community.adapters.openai import convert_message_to_dict

                    full_prompt = [convert_message_to_dict(m) for m in messages]
                    return full_prompt
                else:
                    full_prompt: str
                    full_prompt = "\n".join([m.content for m in messages])
                    return full_prompt

            @property
            def _llm_type(self) -> str:
                """Return type of chat model."""
                return "CFG AI"
            
        return ChatCfgAI(model_engine=self)
    
