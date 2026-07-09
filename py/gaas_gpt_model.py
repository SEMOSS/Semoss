import os
import json
import warnings
from typing import List, Optional, Dict, Union, Any
from abc import ABC, abstractmethod
from gaas_server_proxy import ServerProxy


class AbstractModelEngine(ABC):
    """This is an abstract class the defined what methods need to be implemeted for a ModelEngine"""

    @abstractmethod
    def get_model_type(self, *args: Any, **kwargs: Any) -> str:
        """This method is to know the type of API being used to connect to the model"""
        pass

    @abstractmethod
    def ask(self, *args: Any, **kwargs: Any) -> List[Dict]:
        """This method is responsible for interacting with models that can perform text-generation"""
        pass

    @abstractmethod
    def embeddings(self, *args: Any, **kwargs: Any) -> List[Dict]:
        """This method is responsible for interacting with models that can create embeddings from strings"""
        pass

    @abstractmethod
    def keyword_extraction(self, *args: Any, **kwargs: Any) -> List[Any]:
        """This method is responsible for interacting with models that can perform keyword extraction (like keyBERT)"""
        pass

    @abstractmethod
    def do_call(self, method_name: str, input: Any, **kwargs: Any) -> Any:
        """This method is responsible for utilizing a specific tokenize function that is unique to that tokenize function"""
        pass

    @abstractmethod
    def get_model_engine_id(self) -> str:
        """This method returns the model engine id of the `AbstractModelEngine` class. If the engine has not been set then it returns `None`."""
        pass


class TomcatModelEngine(AbstractModelEngine, ServerProxy):
    """This class implements AbstractModelEngine class and is used as the "ModelEngine" class when calling `from gaas_gpt_model import ModelEngine` from a python
    process in Tomcat Server"""

    def __init__(
        self,
        engine_id: str = None,
        insight_id: Optional[str] = None,
    ):
        """
        Initialize the TomcatModelEngine instance.

        Args:
            engine_id (`str`): Identifier of the model engine.
            insight_id (`Optional[str]`): Identifier for insights.
            local (`Optional[bool]`): Whether the model runs locally.
            pipeline_type (`Optional[str]`): Type of pipeline for local models.
            **kwargs: Additional keyword arguments.
        """
        assert engine_id is not None
        super().__init__()  # initialize the ServerProxy class
        self.engine_id = engine_id  # set the engine id
        if insight_id is None:
            insight_id = super().get_thread_insight_id()
        self.insight_id = insight_id  # set the insight id

    def get_model_type(self, insight_id: Optional[str] = None):
        """This method is responsible for returning the model API being used
        Args:
            - insight_id (Optional[str]): Identifier for insights.

        Returns:
            `str`: The type of the API that corresponds to ModelTypeEnum
        """

        if insight_id is None:
            insight_id = self.insight_id

        epoc = super().get_next_epoc()

        pixel = f'GetModelAPI(model="{self.engine_id}");'

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            if "ERROR" in output["operationType"]:
                raise Exception(output["output"])
            return output["output"]

        return pixelReturn

    def ask(
        self,
        command: Optional[str] = None,
        question: Optional[str] = None,  # Deprecated
        room_id: Optional[str] = None,
        context: Optional[str] = None,
        image: Optional[List] = None,
        url: Optional[List] = None,
        use_history: Optional[bool] = True,
        param_dict: Optional[Dict] = None,
        insight_id: Optional[str] = None,
    ) -> List[Dict]:
        """This method is responsible for interacting with models

        Args:
            - command (str): The command to send to the model.
            - question (str): **Deprecated**. Use `command` instead.
            - room_id (Optional[str]): Identifier for the room/conversation. If not provided, one will be created on the Java side.
            - context (Optional[str]): Context for the model (the system prompt).
            - image (Optional[List]): List of base64 image data to provide to the model.
            - url (Optional[List]): List of image URLs to provide to the model.
            - use_history (Optional[bool]): Whether to provide the conversation history to the model on an individual call.
            - param_dict (Optional[Dict]): Additional parameters.
            - insight_id (Optional[str]): Identifier for insights.

        Returns:
            `List[Dict]`: A dictionary with the response from the text-generation model. The dictionary in the response will contain the following keys:
            - response
            - numberOfTokensInPrompt
            - numberOfTokensInResponse
            - messageId
            - roomId
        """

        if question is not None:
            warnings.warn(
                "The 'question' parameter is deprecated and will be removed in a future version. "
                "Please use 'command' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if command is None:
                command = question

        if insight_id is None:
            insight_id = self.insight_id

        epoc = super().get_next_epoc()

        command_param = (
            f',command="<encode>{command}</encode>"' if (command is not None) else ""
        )

        if (
            command_param == ""
            and param_dict is not None
            and not param_dict.get("full_prompt", None)
        ):
            raise ValueError("Either command or question must be provided")

        optional_room_id_param = (
            f',roomId="<encode>{room_id}</encode>"' if (room_id is not None) else ""
        )
        optional_context = (
            f',context=["<encode>{context}</encode>"]' if (context is not None) else ""
        )
        optional_param_dict = (
            f",paramValues=[{json.dumps(param_dict, ensure_ascii=False)}]"
            if (param_dict is not None)
            else ""
        )
        optional_image_param = f",image={image}" if (image is not None) else ""
        optional_url_param = f",url={url}" if (url is not None) else ""
        optional_use_history_param = (
            f", useHistory={str(use_history).lower()}"
            if (use_history is not None)
            else ""
        )

        pixel = f'LLM(engine="{self.engine_id}"{command_param}{optional_context}{optional_use_history_param}{optional_param_dict}{optional_room_id_param}{optional_image_param}{optional_url_param});'

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            # prior to reactor we were returning this in an array
            # keeping for backward compatibility
            if "ERROR" in output["operationType"]:
                raise Exception(output["output"])
            return [output["output"]]

        return pixelReturn

    def ner(
        self,
        text: str,
        entities: List[str],
        mask_entities: List[str] = [],
        param_dict: Optional[Dict] = None,
        insight_id: Optional[str] = None,
    ):
        if insight_id is None:
            insight_id = self.insight_id

        epoc = super().get_next_epoc()

        pixel = f'NER(engine="{self.engine_id}", prompt="{text}", entities={entities}, maskEntities={mask_entities});'

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            if "ERROR" in output["operationType"]:
                raise Exception(output["output"])
            return output["output"]

        return pixelReturn

    def get_conversation_history(self, insight_id: Optional[str] = None) -> List[Dict]:
        """This method is responsible to get message history back from the model logs database.

        Args:
            - insight_id (Optional[str]): Identifier for insights.

        Returns:
            `List[Dict]`: A dictionary with the response the model logs database. The dictionary in the response will contain the following keys:
            - MESSAGE_DATA
            - DATE_CREATED
            - MESSAGE_ID
            - MESSAGE_TYPE
        """

        if insight_id is None:
            insight_id = self.insight_id

        epoc = super().get_next_epoc()

        pixel = f'GetRoomMessages(roomId="{insight_id}");'

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            if "ERROR" in output["operationType"]:
                raise Exception(output["output"])
            return output["output"]

        return pixelReturn

    def embeddings(
        self,
        strings_to_embed: List[str],
        param_dict: Optional[Dict] = None,
        insight_id: Optional[str] = None,
    ) -> List[Dict]:
        if insight_id is None:
            insight_id = self.insight_id

        if isinstance(strings_to_embed, str):
            strings_to_embed = [strings_to_embed]

        assert isinstance(strings_to_embed, list)
        encoded_string = [f"<encode>{s}</encode>" for s in strings_to_embed]

        epoc = super().get_next_epoc()

        optionalParamDict = (
            f",paramValues=[{json.dumps(param_dict, ensure_ascii=False)}]"
            if (param_dict is not None)
            else ""
        )

        pixel = f'Embeddings(engine="{self.engine_id}",values={encoded_string}{optionalParamDict},encoded=true);'

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            # prior to reactor we were returning this in an array
            # keeping for backward compatibility
            if "ERROR" in output["operationType"]:
                raise Exception(output["output"])
            return [output["output"]]

        return pixelReturn

    def image_embeddings(
        self,
        images_to_embed: List[str],
        param_dict: Optional[Dict] = None,
        insight_id: Optional[str] = None,
    ) -> List[Dict]:
        if insight_id is None:
            insight_id = self.insight_id

        if isinstance(images_to_embed, str):
            images_to_embed = [images_to_embed]

        assert isinstance(images_to_embed, list)

        epoc = super().get_next_epoc()

        optionalParamDict = (
            f",paramValues=[{json.dumps(param_dict, ensure_ascii=False)}]"
            if (param_dict is not None)
            else ""
        )

        pixel = f'ImageEmbeddings(engine="{self.engine_id}", values={images_to_embed}{optionalParamDict});'

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            if "ERROR" in output["operationType"]:
                raise Exception(output["output"])
            return output["output"]

        return pixelReturn

    def keyword_extraction(
        self,
        input: List[str],
        param_dict: Optional[Dict] = None,
        insight_id: Optional[str] = None,
    ):
        if insight_id is None:
            insight_id = self.insight_id

        epoc = super().get_next_epoc()

        encoded_input = []
        for ele in input:
            encoded_input.append(f"<encode>{ele}</encode>")
        percentile = ""
        max_keywords = ""

        if param_dict["percentile"] is not None:
            percentile = f', percentile={param_dict["percentile"]}'
        if param_dict["max_keywords"] is not None:
            max_keywords = f', limit={param_dict["max_keywords"]}'

        pixel = f'EmbedderKeywordExtraction(model="{self.engine_id}", input={encoded_input}{percentile}{max_keywords});'

        pixelReturn = super().callReactor(
            epoc=epoc,
            pixel=pixel,
            insight_id=insight_id,
        )

        if pixelReturn is not None and len(pixelReturn) > 0:
            output = pixelReturn[0]["pixelReturn"][0]
            if "ERROR" in output["operationType"]:
                raise Exception(output["output"])
            return output["output"]

        return pixelReturn

    # this is a little bit of get out of jail free card
    def do_call(self, method_name: str, input: Any, **kwargs) -> Any:
        call_maker = getattr(self, method_name, None)
        if call_maker is not None:
            return call_maker(input, **kwargs)
        else:
            return None

    def get_model_engine_id(self) -> str:
        return self.engine_id


class HuggingFacePipelineModelEngine(AbstractModelEngine):
    def __init__(self, engine_id: str, pipeline_type: Optional[str] = None, **kwargs):
        """
        Initialize the TomcatModelEngine instance.

        Args:
            engine_id (`str`): Identifier of the model engine.
            insight_id (`Optional[str]`): Identifier for insights.
            local (`Optional[bool]`): Whether the model runs locally.
            pipeline_type (`Optional[str]`): Type of pipeline for local models.
            **kwargs: Additional keyword arguments.
        """
        super().__init__()  # initialize the ServerProxy class
        self.engine_id = engine_id  # set the engine id

        # start the model and make it available
        import torch
        from transformers import pipeline

        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.pipeline_type = pipeline_type
        self.pipe = pipeline(pipeline_type, model=engine_id, device=device)

    def get_model_type(self, insight_id: Optional[str] = None):
        return self.pipeline_type

    def ask(
        self,
        question: str,
        context: Optional[str] = None,
        param_dict: Optional[Dict] = None,
    ) -> List[Dict]:
        """This method is responsible for interacting with models that can perform text-generation

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
        """
        raise NotImplementedError(
            "HuggingFacePipelineModelEngine does not have an ask method implemented"
        )

    def embeddings(
        self, strings_to_embed: List[str], param_dict: Optional[Dict] = None
    ) -> List[Dict]:
        return self.pipe.model.encode(strings_to_embed)

    def keyword_extraction(
        self,
        input: Any,
        param_dict: Optional[Dict] = None,
    ):
        return self.pipe(input)

    # this is a little bit of get out of jail free card
    def do_call(self, method_name: str, input: Any, **kwargs) -> Any:
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
        semoss_dev_path: Optional[str] = (
            "C:/workspace/Semoss_Dev" if os.name == "nt" else "/opt/semosshome"
        ),
    ):
        # determine how to create the model engine locally
        if engine_smss_file_path is not None or engine_id is not None:
            if engine_smss_file_path is not None:
                # the direct path of the smss file was passed in
                pass
            elif engine_id is not None:
                # the gave the engine id so try to find the smss file
                engine_smss_file_path = LocalModelEngine.get_model_smss_file(
                    semoss_dev_path, engine_id
                )

            # get the smss props from the smss file
            smss_props = LocalModelEngine.read_smss_file(engine_smss_file_path)

            # use the smss props to initialize the model engine
            model_engine_init_command = LocalModelEngine.get_init_model_commads(
                smss_props
            )
            exec(model_engine_init_command)

            self.local_model_engine = locals().get(smss_props["VAR_NAME"], None)
            self.engine_id = smss_props.get("ENGINE", None)
        else:
            self.local_model_engine = model_engine
            self.engine_id = None

        assert (
            self.local_model_engine != None
        ), "Unable to define a Local Model Engine based on the parameters passed in"

    def get_model_type(self, *args, **kwargs):
        return [self.local_model_engine.get_model_type(**kwargs)]

    def ask(self, **kwargs) -> Dict:
        return [self.local_model_engine.ask(**kwargs)]

    def embeddings(self, **kwargs) -> Dict:
        return [self.local_model_engine.embeddings(**kwargs)]

    def keyword_extraction(self, **kwargs):
        return [self.local_model_engine.keyword_extraction(**kwargs)]

    def do_call(self, method_name: str, input: Any, **kwargs) -> Any:
        call_maker = getattr(self, method_name, None)
        if call_maker is not None:
            return call_maker(input, **kwargs)
        else:
            return None

    def get_model_engine_id(self) -> str:
        return self.engine_id

    @staticmethod
    def get_model_smss_file(semoss_dev_file_path: str, engine_id: str) -> str:
        """This method returns a list of smss files in the semosshome model directory"""
        import glob

        file_pattern = "*.smss"

        # Use glob.glob to find all matching files in the directory
        model_smss_files = glob.glob(
            os.path.join(semoss_dev_file_path, "model", file_pattern)
        )
        model_smss_files.sort()

        for smss_file_name in model_smss_files:
            if smss_file_name.find(engine_id) > 0:
                return smss_file_name

        raise ValueError(f"Unable to find smss file for the engine id: {engine_id}")

    @staticmethod
    def read_smss_file(file_path: str) -> Dict:
        smss_props = {}
        with open(file_path.replace("\\", "/"), "r") as file:
            for line in file:
                line = line.strip()
                if line.startswith("#") or len(line) == 0:
                    continue
                try:
                    key, value = line.split(None, 1)  # Split by any whitespace
                except:
                    pass
                smss_props[key] = (
                    True
                    if value.lower() == "true"
                    else (
                        False if value.lower() == "false" else value.replace("\\", "/")
                    )
                )
        return smss_props

    @staticmethod
    def get_init_model_commads(smss_props: dict) -> str:
        import re

        init_model_engine_template = smss_props["INIT_MODEL_ENGINE"]

        # Find all placeholders in the template string
        placeholders = re.findall(r"\${(.*?)}", init_model_engine_template)

        # Create a dictionary with the actual values for the found placeholders
        values = {
            placeholder: smss_props[placeholder]
            for placeholder in placeholders
            if placeholder in smss_props
        }

        # Substitute the placeholders with the actual values from the dictionary
        formatted_string_dynamic = init_model_engine_template
        for placeholder, value in values.items():
            formatted_string_dynamic = formatted_string_dynamic.replace(
                "${" + placeholder + "}", value
            )

        return formatted_string_dynamic


class ModelEngine(AbstractModelEngine):
    def __init__(self, model_engine_class: Optional[str] = "TOMCAT", **kwargs):
        if model_engine_class == "TOMCAT":
            self.model_engine = TomcatModelEngine(**kwargs)
        elif model_engine_class == "LOCAL":
            self.model_engine = LocalModelEngine(**kwargs)
        elif model_engine_class == "HF_PIPELINE":
            pass
        else:
            raise ValueError(
                "Unable to define a Model Engine. Model Engine Class types are 'TOMCAT', 'LOCAL', or 'HF_PIPELINE'."
            )

    def get_model_type(self, insight_id: Optional[str] = None, **kwargs) -> str:
        return self.model_engine.get_model_type(insight_id, **kwargs)

    def ask(
        self,
        insight_id: Optional[
            str
        ] = None,  # TODO remove once users stop using it. No longer needs to be set.
        **kwargs,
    ) -> Dict:
        return self.model_engine.ask(**kwargs)

    def embeddings(
        self,
        insight_id: Optional[
            str
        ] = None,  # TODO remove once users stop using it. No longer needs to be set.
        **kwargs,
    ) -> Dict:
        return self.model_engine.embeddings(**kwargs)

    def image_embeddings(
        self,
        insight_id: Optional[
            str
        ] = None,  # TODO remove once users stop using it. No longer needs to be set.
        **kwargs,
    ) -> Dict:
        return self.model_engine.image_embeddings(**kwargs)

    def keyword_extraction(
        self,
        insight_id: Optional[
            str
        ] = None,  # TODO remove once users stop using it. No longer needs to be set.
        **kwargs,
    ):
        return self.model_engine.keyword_extraction(**kwargs)

    def ner(
        self,
        insight_id: Optional[
            str
        ] = None,  # TODO remove once users stop using it. No longer needs to be set.
        **kwargs,
    ):
        return self.model_engine.ner(**kwargs)

    def do_call(self, **kwargs: Any) -> Any:
        return self.model_engine.embeddings(**kwargs)

    def get_model_engine_id(self) -> str:
        return self.model_engine.get_model_engine_id()

    def get_conversation_history(self):
        return self.model_engine.get_conversation_history()

    def to_langchain_embedder(self):
        """Transform the model engine into a langchain `Embeddings`object so that it can be used with langchain code"""

        from langchain_core.embeddings import Embeddings

        class SemossLangchainEmbeddingsModel(Embeddings):
            def __init__(self, modelEngine):
                self.modelEngine = modelEngine

            def embed_documents(self, texts: List[str]) -> List[List[float]]:
                """Embed search docs."""
                return self.modelEngine.embeddings(strings_to_embed=texts)[0][
                    "response"
                ]

            def embed_query(self, text: str) -> List[float]:
                return self.modelEngine.embeddings(strings_to_embed=[text])[0][
                    "response"
                ][0]

        return SemossLangchainEmbeddingsModel(modelEngine=self)

    def to_langchain_chat_model(self):
        """Transform the model engine into a langchain `BaseChatModel` object so that it can be used with langchain code"""
        from langchain_core.language_models.chat_models import BaseChatModel
        from langchain_core.outputs import (
            ChatGeneration,
            ChatResult,
        )
        from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

        class SemossLangchainChatModel(BaseChatModel):
            engine_id: str
            model_engine: ModelEngine
            model_type: str

            def __init__(self, model_engine):
                data = {
                    "engine_id": model_engine.get_model_engine_id(),
                    "model_engine": model_engine,
                    "model_type": model_engine.get_model_type(),
                }
                super().__init__(**data)

            def get_chat_history(
                self, insight_id: Optional[str] = None
            ) -> List[BaseMessage]:
                """Retrieve past conversation history and format it for Langchain."""

                # Fetch chat history from ModelEngine
                history = self.model_engine.get_conversation_history()
                messages = []
                for msg in sorted(history, key=lambda x: x["DATE_CREATED"]):
                    if msg["MESSAGE_TYPE"] == "INPUT":
                        messages.append(HumanMessage(content=msg["MESSAGE_DATA"]))
                    elif msg["MESSAGE_TYPE"] == "RESPONSE":
                        messages.append(AIMessage(content=msg["MESSAGE_DATA"]))
                return messages

            class Config:
                """Configuration for this pydantic object."""

                allow_population_by_field_name = True

            def bind_tools(
                self,
                tools: List[Any],
                *,
                tool_choice: Optional[Any] = None,
                **kwargs: Any,
            ):
                """Bind LangChain tools onto the chat model.

                Tools are converted to the OpenAI function-schema shape SEMOSS
                already normalizes on (see ``semoss_base.semoss_message_builder``
                for the canonical tool_call dict). This is what makes the model
                usable inside ``langgraph.prebuilt.create_react_agent`` and any
                downstream framework that speaks LangChain's tool-calling
                protocol.
                """
                from langchain_core.utils.function_calling import (
                    convert_to_openai_tool,
                )

                formatted = [convert_to_openai_tool(t) for t in tools]
                bind_kwargs: Dict[str, Any] = {"tools": formatted, **kwargs}
                if tool_choice is not None:
                    bind_kwargs["tool_choice"] = tool_choice
                return self.bind(**bind_kwargs)

            def _generate(
                self,
                messages: List[BaseMessage],
                stop: Optional[List[str]] = None,
                **kwargs: Any,
            ) -> ChatResult:
                """Top Level call"""
                history = self.get_chat_history()

                # Combine history with new messages (if history exists)
                full_messages = history + messages if history else messages

                # Convert to appropriate prompt format
                full_prompt = self.convert_messages_to_full_prompt(full_messages)

                # Send the combined prompt to the model
                response = self.model_engine.ask(
                    command="", param_dict={**kwargs, **{"full_prompt": full_prompt}}
                )

                return self._create_chat_result(response=response[0])

            def _extract_tool_calls(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
                """Return LangChain-shaped tool_calls from a raw model response.

                Handles the three shapes SEMOSS providers commonly return:
                openai-style ``tool_calls``, anthropic-style ``tool_use``
                blocks, and gemini-style ``function_calls``. Returns ``[]``
                when nothing tool-shaped is present.
                """
                import json as _json

                raw = (
                    response.pop("tool_calls", None)
                    or response.pop("tool_uses", None)
                    or response.pop("function_calls", None)
                )
                if not raw:
                    return []

                normalized: List[Dict[str, Any]] = []
                for i, item in enumerate(raw):
                    fn = item.get("function") or item
                    name = fn.get("name") or item.get("name")
                    args = fn.get("arguments") or item.get("input") or {}
                    if isinstance(args, str):
                        try:
                            args = _json.loads(args)
                        except Exception:
                            args = {"_raw": args}
                    if not name:
                        continue
                    normalized.append(
                        {
                            "name": name,
                            "args": args,
                            "id": str(item.get("id") or f"call_{i}"),
                            "type": "tool_call",
                        }
                    )
                return normalized

            def _create_chat_result(self, response: Dict[str, Any]) -> ChatResult:
                generations = []

                message = response.pop("response", "")
                generation_info = dict()
                if "logprobs" in response.keys():
                    generation_info["logprobs"] = response.pop("logprobs", {})

                # Some providers (e.g. Anthropic here) return tool_use as the
                # response CONTENT itself -- a list of tool-call dicts -- rather
                # than under a tool_calls/tool_uses key. Surface those as real
                # tool_calls so langgraph's react loop routes to the tools
                # instead of ending with the tool-call list as the answer.
                if isinstance(message, list):
                    response["tool_calls"] = message
                    message = ""

                tool_calls = self._extract_tool_calls(response)
                ai_kwargs: Dict[str, Any] = {"content": message}
                if tool_calls:
                    ai_kwargs["tool_calls"] = tool_calls

                gen = ChatGeneration(
                    message=AIMessage(**ai_kwargs),
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

                if self.model_type in ["OPEN_AI", "VERTEX", "ANTHROPIC", "BEDROCK"]:
                    import json as _json

                    full_prompt: List[Dict[str, Any]]
                    from langchain_community.adapters.openai import (
                        convert_message_to_dict,
                    )

                    full_prompt = []
                    for m in messages:
                        d = convert_message_to_dict(m)
                        # convert_message_to_dict only serializes tool calls from
                        # additional_kwargs, NOT the structured AIMessage.tool_calls
                        # that langgraph populates -- so on multi-turn tool loops the
                        # assistant's tool_use vanishes and SEMOSS rejects the paired
                        # tool_result. Re-attach tool_calls in OpenAI shape.
                        tcs = getattr(m, "tool_calls", None)
                        if tcs and not d.get("tool_calls"):
                            d["tool_calls"] = [
                                {
                                    "id": tc.get("id"),
                                    "type": "function",
                                    "function": {
                                        "name": tc.get("name"),
                                        "arguments": _json.dumps(tc.get("args", {})),
                                    },
                                }
                                for tc in tcs
                            ]
                        full_prompt.append(d)
                    return full_prompt
                else:
                    full_prompt: str
                    full_prompt = "\n".join([m.content for m in messages])
                    return full_prompt

            @property
            def _llm_type(self) -> str:
                """Return type of chat model."""
                return "SEMOSS"

        return SemossLangchainChatModel(model_engine=self)
