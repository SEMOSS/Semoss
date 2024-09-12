import boto3
import json
import logging
import requests

from .abstract_text_generation_client import AbstractTextGenerationClient
from ..tokenizers.huggingface_tokenizer import HuggingfaceTokenizer
from ..constants import (
    MAX_TOKENS,
    MAX_INPUT_TOKENS,
    FULL_PROMPT,
    AskModelEngineResponse,
    EmbeddingsModelEngineResponse,
)

# from langchain_community.llms import Bedrock
from langchain_aws.llms import BedrockLLM
from langchain.chains.combine_documents.stuff import StuffDocumentsChain
from langchain.chains.llm import LLMChain
from langchain.docstore.document import Document
from langchain_core.prompts import PromptTemplate
from langchain_community.document_loaders.csv_loader import CSVLoader
from langchain.chains import MapReduceDocumentsChain, ReduceDocumentsChain
from langchain_text_splitters import CharacterTextSplitter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BedrockClient(AbstractTextGenerationClient):
    def __init__(
        self,
        template=None,
        service_name="bedrock-runtime",
        modelId="anthropic.claude-instant-v1",
        access_key=None,
        secret_key=None,
        region=None,
        template_name=None,
        response_stream=False,
        guardrail_identifier=None,
        guardrail_version=None,
        **kwargs,
    ):
        super().__init__(template=template, template_name=template_name)
        self.kwargs = kwargs
        self.modelId = modelId
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service_name = service_name
        self.response_stream = response_stream
        self.guardrail_identifier = guardrail_identifier
        self.guardrail_version = guardrail_version

        # hard code the tokenizer for now
        self.tokenizer = HuggingfaceTokenizer(
            encoder_name="bert-base-uncased",
            max_tokens=kwargs.pop(MAX_TOKENS, None),
            max_input_tokens=kwargs.pop(MAX_INPUT_TOKENS, None),
        )

    def _get_client(self):
        if self.access_key and self.secret_key:
            return boto3.client(
                service_name=self.service_name,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
            )
        else:
            return boto3.client(
                # assuming this is environment auth
                service_name=self.service_name,
                region_name=self.region,
            )

    def create_json_body(self, prompt, max_new_tokens, temperature, top_p):
        # Create a dictionary with the desired parameters
        body_dict = {
            "prompt": prompt,
            "max_tokens_to_sample": max_new_tokens,
            "temperature": temperature,
            "top_p": top_p,
        }

        # Filter out parameters with null or empty values
        filtered_body_dict = {
            key: value
            for key, value in body_dict.items()
            if value is not None and value != ""
        }

        # Convert the filtered dictionary to JSON
        body_json = json.dumps(
            filtered_body_dict
        )  # Optional: indent for pretty printing

        return body_json

    def create_inference_config(self, max_new_tokens, temperature, top_p):
        if top_p is None:
            top_p = 0.9

        if temperature is None:
            temperature = 0.9

        # Base inference parameters to use.
        inference_config = {
            "temperature": temperature,
            "maxTokens": max_new_tokens,
            "topP": top_p,
        }

        return inference_config

    def create_json_body(self, prompt, max_new_tokens, temperature, top_p):
        # Create a dictionary with the desired parameters
        body_dict = {
            "prompt": prompt,
            "max_tokens_to_sample": max_new_tokens,
            "temperature": temperature,
            "top_p": top_p,
        }

        # Filter out parameters with null or empty values
        filtered_body_dict = {
            key: value
            for key, value in body_dict.items()
            if value is not None and value != ""
        }

        # Convert the filtered dictionary to JSON
        body_json = json.dumps(
            filtered_body_dict
        )  # Optional: indent for pretty printing

        return body_json

    def create_json_body_titan(
        self, prompt, max_new_tokens, temperature, top_p, stop_sequences
    ):
        # Create a dictionary with the desired parameters
        if stop_sequences is None:
            stop_sequences = []

        if top_p is None:
            top_p = 0.9

        if max_new_tokens is None:
            max_new_tokens = 200

        if temperature is None:
            temperature = 0.9

        body_dict = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": max_new_tokens,
                "stopSequences": stop_sequences,
                "temperature": temperature,
                "topP": top_p,
            },
        }

        # Filter out parameters with null or empty values
        filtered_body_dict = {
            key: value
            for key, value in body_dict.items()
            if value is not None and value != ""
        }

        # Convert the filtered dictionary to JSON
        body_json = json.dumps(
            filtered_body_dict
        )  # Optional: indent for pretty printing

        return body_json

    def summarize(self, **kwargs):
        client = self._get_client()
        model_engine_response = AskModelEngineResponse()
        llm = BedrockLLM(model_id=self.modelId, region_name="us-east-1", client=client)

        csv_path = kwargs["file_path"]
        loader = CSVLoader(
            file_path=csv_path,
            csv_args={
                "delimiter": ",",
                "quotechar": '"',
                "fieldnames": ["Content"],
            },
        )
        docs = loader.load()
        map_template = """The following is a set of documents
            {docs}
            Based on this list of docs, please identify the main themes 
            Helpful Answer:"""
        map_prompt = PromptTemplate.from_template(map_template)
        map_chain = LLMChain(llm=llm, prompt=map_prompt)

        reduce_template = """The following is set of summaries:
            {docs}
            Take these and distill it into a final, consolidated summary of the main themes. 
            Helpful Answer:"""
        reduce_prompt = PromptTemplate.from_template(reduce_template)

        reduce_chain = LLMChain(llm=llm, prompt=reduce_prompt)

        # Combine documents into a string
        combine_documents_chain = StuffDocumentsChain(
            llm_chain=reduce_chain, document_variable_name="docs"
        )

        # Combines and iteratively reduces the mapped documents
        reduce_documents_chain = ReduceDocumentsChain(
            # Final chain called
            combine_documents_chain=combine_documents_chain,
            # If documents exceed context limit
            collapse_documents_chain=combine_documents_chain,
            # For Titan this could possibly be set to 8k
            token_max=4000,
        )

        # Combining documents by mapping a chain over them, then combining results
        map_reduce_chain = MapReduceDocumentsChain(
            llm_chain=map_chain,
            reduce_documents_chain=reduce_documents_chain,
            document_variable_name="docs",
            return_intermediate_steps=False,
        )

        text_splitter = CharacterTextSplitter.from_tiktoken_encoder(
            chunk_size=1000, chunk_overlap=0
        )
        split_docs = text_splitter.split_documents(docs)

        summary_results = map_reduce_chain.invoke(split_docs)

        # results_map = {}
        # results_map["response"] = summary_results["output_text"]
        # results_map["file_path"] = csv_path
        final_response = summary_results["output_text"]
        model_engine_response.response_tokens = self.tokenizer.count_tokens(
            final_response
        )
        # model_engine_response.prompt_tokens = self.tokenizer.count_tokens(summary_results["input_documents"][0])
        model_engine_response.response = final_response

        return model_engine_response

    def ask_call(
        self,
        question=None,
        context=None,
        template_name=None,
        history=None,
        max_new_tokens=500,
        temperature=None,
        top_p=None,
        stop_sequences=None,
        prefix="",
        **kwargs,
    ) -> AskModelEngineResponse:
        client = self._get_client()
        final_response = ""
        model_engine_response = AskModelEngineResponse()

        # TODO remove once
        # check whether to include logprobs in the response
        include_logprobs = kwargs.pop("include_logprobs", False)
        try:
            message_payload = []
            # Common variable assignment for both chat-completion and completion
            mapping = {"question": question} | kwargs
            prompt_content = ""

            if FULL_PROMPT not in kwargs.keys():
                if context and not template_name:
                    if isinstance(context, str):
                        context = self.fill_context(context, **mapping)[0]
                        message_payload.append({"role": "system", "content": context})
                elif context and template_name:
                    mapping.update({"context": context})
                    context = self.fill_template(
                        template_name=template_name, **mapping
                    )[0]
                    message_payload.append({"role": "system", "content": context})
                else:
                    if template_name:
                        possibleContent = self.fill_template(
                            template_name=template_name, **mapping
                        )[0]
                        if possibleContent:
                            message_payload.append(
                                {"role": "system", "content": possibleContent}
                            )

                if history is not None:
                    message_payload.extend(history)

                if question and len(question) > 0:
                    message_payload.append({"role": "user", "content": question})

                kwargs["messages"] = message_payload

                msg_content = "\n\nHuman:".join(
                    [msg["content"] for msg in message_payload]
                )
                prompt_content = "\n\nHuman:" + msg_content + "\n\nAssistant:"
            else:
                if self.modelId == "anthropic.claude-instant-v1":
                    prompt_content = (
                        "\n\nHuman:" + kwargs[FULL_PROMPT] + "\n\nAssistant:"
                    )
                elif self.modelId == "amazon.titan-text-express-v1":
                    prompt_content = kwargs[FULL_PROMPT]

            model_engine_response.prompt_tokens = self.tokenizer.count_tokens(
                prompt_content
            )
            messages = [{"role": "user", "content": [{"text": prompt_content}]}]

            inference_config = self.create_inference_config(
                max_new_tokens, temperature, top_p
            )

            if self.response_stream == True:
                if (
                    self.guardrail_identifier is not None
                    and self.guardrail_version is not None
                ):
                    guardrail_config = {
                        "guardrailIdentifier": self.guardrail_identifier,
                        "guardrailVersion": self.guardrail_version,
                        "trace": "enabled",
                    }

                    response = client.converse_stream(
                        modelId=self.modelId,
                        messages=messages,
                        guardrailConfig=guardrail_config,
                        inferenceConfig=inference_config,
                    )
                else:
                    response = client.converse_stream(
                        modelId=self.modelId,
                        messages=messages,
                        # system=system_prompts,
                        inferenceConfig=inference_config,
                        # additionalModelRequestFields=additional_model_fields
                    )

                stream = response.get("stream")
                if stream:
                    for event in stream:
                        if "contentBlockDelta" in event:
                            full_response += event["contentBlockDelta"]["delta"]["text"]

                    model_engine_response.response_tokens = self.tokenizer.count_tokens(
                        final_response
                    )

            else:
                if (
                    self.guardrail_identifier is not None
                    and self.guardrail_version is not None
                ):
                    guardrail_config = {
                        "guardrailIdentifier": self.guardrail_identifier,
                        "guardrailVersion": self.guardrail_version,
                        "trace": "enabled",
                    }

                    response = client.converse(
                        modelId=self.modelId,
                        messages=messages,
                        guardrailConfig=guardrail_config,
                        inferenceConfig=inference_config,
                    )
                else:
                    response = client.converse(
                        modelId=self.modelId,
                        messages=messages,
                        inferenceConfig=inference_config,
                    )

                output_message = response["output"]["message"]["content"]

                if len(output_message) > 0:
                    final_response = output_message[0]["text"]

                model_engine_response.response_tokens = response["usage"][
                    "outputTokens"
                ]

            model_engine_response.response = final_response
            return model_engine_response

        except Exception as e:
            logger.error(f"Error while making request to Bedrock: {e}")
            raise Exception(f"Error while making request to Bedrock: {e}")

        return final_response
    
    def embeddings_call(self, strings_to_embed:list[str]) -> EmbeddingsModelEngineResponse:  
        embeddings_list = []
        embeddings = []       

        for text in strings_to_embed:
            json_obj = {"inputText": text}
            request = json.dumps(json_obj)

            try:   
                client = self._get_client()             

                response = client.invoke_model(
                    modelId=self.modelId, body=request
                )
                response_body = json.loads(response['body'].read()) 
                embedding_array = response_body.get("embedding")

                if embedding_array:
                    embeddings_list = [float(value) for value in embedding_array]
                    embeddings.append(embeddings_list)

                model_engine_response = EmbeddingsModelEngineResponse(
                response=embeddings,
                prompt_tokens=response_body.get("inputTextTokenCount"),
                response_tokens=0
            )

            except requests.RequestException as e:
                print(f"An error occurred in bedrock embeddings_call: {e}")                 

        return model_engine_response
