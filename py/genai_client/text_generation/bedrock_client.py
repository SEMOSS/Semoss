import boto3
import logging
from typing import List, Optional, Tuple, Dict

from .abstract_text_generation_client import AbstractTextGenerationClient
from ..tokenizers.huggingface_tokenizer import HuggingfaceTokenizer
from ..constants import (
    MAX_TOKENS,
    MAX_INPUT_TOKENS,
    FULL_PROMPT,
    IMAGE_ENCODED,
    AskModelEngineResponse,
)


from langchain_community.llms import Bedrock
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
        response_stream=None,
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
        self.botoClient = self._get_client()

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

        final_response = summary_results["output_text"]
        model_engine_response.response_tokens = self.tokenizer.count_tokens(
            final_response
        )
        # model_engine_response.prompt_tokens = self.tokenizer.count_tokens(summary_results["input_documents"][0])
        model_engine_response.response = final_response

        return model_engine_response

    def ask_call(
        self,
        question: str =None,
        context: str =None,
        template_name: str =None,
        history: List[Dict] = None,
        # We should now expect max_completion_tokens but I can't get rid of this yet..
        max_new_tokens=None, #Deprecated,
        max_completion_tokens=None,
        temperature=None,
        top_p=None,
        stop_sequences=None,
        prefix="",
        stream = True,
        **kwargs,
    ) -> AskModelEngineResponse:

        if template_name == None:
            template_name = self.template_name


        # first we determine the type of completion, since this determines how we structure the payload
        message_payload = []

        if FULL_PROMPT not in kwargs.keys():

            message_payload = self._process_chat_completion(
                question=question,
                context=context,
                history=history,
                template_name=template_name,
                fill_variables=kwargs,
            )
        else:
            message_payload = self._process_full_prompt(kwargs.pop(FULL_PROMPT))

        # We want to honor the new variable name first, then the old variable name but its okay if both are None
        # Once everyone is using the new variable name, we can remove this
        user_max_tokens = (
            max_completion_tokens
            if max_completion_tokens is not None
            else max_new_tokens
        )

        # Check to see if we need to truncate the prompt or adjust max_completion_tokens
        prompt, user_max_tokens, model_engine_response = (
            self.check_token_limits(
                prompt_payload=message_payload, user_max_tokens=user_max_tokens
            )
        )

        # Add the message payload as a kwarg
        kwargs["messages"] = prompt

        # check if we need guardrail
        if (
            self.guardrail_identifier is not None
            and self.guardrail_version is not None
        ):
            guardrail_config = {
                "guardrailIdentifier": self.guardrail_identifier,
                "guardrailVersion": self.guardrail_version,
                "trace": "enabled",
            }
            kwargs["guardrailConfig"] = guardrail_config

        # get an inference config
        inference_config = self.create_inference_config(
            user_max_tokens, temperature, top_p
        )
        
        kwargs["inferenceConfig"] = inference_config

        
        response, outputTokens = self._inference_call(
            prefix=prefix,stream=stream, **kwargs
        )
        model_engine_response.response=response
        model_engine_response.response_tokens = outputTokens

        return model_engine_response
    
        # client = self._get_client()
        # final_response = ""
        # model_engine_response = AskModelEngineResponse()

        # # TODO remove once
        # # check whether to include logprobs in the response
        # include_logprobs = kwargs.pop("include_logprobs", False)
        # try:
        #     message_payload = []
        #     # Common variable assignment for both chat-completion and completion
        #     mapping = {"question": question} | kwargs
        #     prompt_content = ""

        #     if FULL_PROMPT not in kwargs.keys():
        #         if context and not template_name:
        #             if isinstance(context, str):
        #                 context = self.fill_context(context, **mapping)[0]
        #                 message_payload.append({"role": "system", "content": context})
        #         elif context and template_name:
        #             mapping.update({"context": context})
        #             context = self.fill_template(
        #                 template_name=template_name, **mapping
        #             )[0]
        #             message_payload.append({"role": "system", "content": context})
        #         else:
        #             if template_name:
        #                 possibleContent = self.fill_template(
        #                     template_name=template_name, **mapping
        #                 )[0]
        #                 if possibleContent:
        #                     message_payload.append(
        #                         {"role": "system", "content": possibleContent}
        #                     )

        #         if history is not None:
        #             message_payload.extend(history)

        #         if question and len(question) > 0:
        #             message_payload.append({"role": "user", "content": question})

        #         # where is this used? 
        #         kwargs["messages"] = message_payload

        #         # why is this here? 
        #         msg_content = "\n\nHuman:".join(
        #             [msg["content"] for msg in message_payload]
        #         )
        #         prompt_content = "\n\nHuman:" + msg_content + "\n\nAssistant:"
        #     else:
        #         if self.modelId == "anthropic.claude-instant-v1":
        #             prompt_content = (
        #                 "\n\nHuman:" + kwargs[FULL_PROMPT] + "\n\nAssistant:"
        #             )
        #         else:
        #             prompt_content = kwargs[FULL_PROMPT]

        #     model_engine_response.prompt_tokens = self.tokenizer.count_tokens(
        #         prompt_content
        #     )
        #     messages = [{"role": "user", "content": [{"text": prompt_content}]}]

        #     inference_config = self.create_inference_config(
        #         max_new_tokens, temperature, top_p
        #     )
            
        #     if stream is None:
        #         stream = self.response_stream

        #     if stream == "true" or stream == True:
        #         if (
        #             self.guardrail_identifier is not None
        #             and self.guardrail_version is not None
        #         ):
        #             guardrail_config = {
        #                 "guardrailIdentifier": self.guardrail_identifier,
        #                 "guardrailVersion": self.guardrail_version,
        #                 "trace": "enabled",
        #             }

        #             response = client.converse_stream(
        #                 modelId=self.modelId,
        #                 messages=messages,
        #                 guardrailConfig=guardrail_config,
        #                 inferenceConfig=inference_config,
        #             )
        #         else:
        #             response = client.converse_stream(
        #                 modelId=self.modelId,
        #                 messages=messages,
        #                 # system=system_prompts,
        #                 inferenceConfig=inference_config,
        #                 # additionalModelRequestFields=additional_model_fields
        #             )

        #         stream_response = response.get("stream")
        #         if stream_response:
        #             for event in stream_response:
        #                 if "contentBlockDelta" in event:
        #                     final_response += event["contentBlockDelta"]["delta"]["text"]

        #             model_engine_response.response_tokens = self.tokenizer.count_tokens(
        #                 final_response
        #             )

        #     else:
        #         if (
        #             self.guardrail_identifier is not None
        #             and self.guardrail_version is not None
        #         ):
        #             guardrail_config = {
        #                 "guardrailIdentifier": self.guardrail_identifier,
        #                 "guardrailVersion": self.guardrail_version,
        #                 "trace": "enabled",
        #             }

        #             response = client.converse(
        #                 modelId=self.modelId,
        #                 messages=message_payload,
        #                 guardrailConfig=guardrail_config,
        #                 inferenceConfig=inference_config,
        #             )
        #         else:
        #             response = client.converse(
        #                 modelId=self.modelId,
        #                 messages=message_payload,
        #                 inferenceConfig=inference_config,
        #             )

        #         output_message = response["output"]["message"]["content"]

        #         if len(output_message) > 0:
        #             final_response = output_message[0]["text"]

        #         model_engine_response.response_tokens = response["usage"][
        #             "outputTokens"
        #         ]
        #     model_engine_response.response = final_response
        #     return model_engine_response

        # except Exception as e:
        #     logger.error(f"Error while making request to Bedrock: {e}")
        #     raise Exception(f"Error while making request to Bedrock: {e}")

    def _process_chat_completion(
        self,
        question: str,
        context: str,
        history: List[Dict],
        template_name: str,
        fill_variables: Dict,
    ) -> List[Dict]:
        # the list to construct the payload from
        message_payload = []

        # if the user provided context, use that. Otherwise, try to get it from the template
        mapping = {"question": question} | fill_variables
        if context is not None and template_name == None:
            if isinstance(context, str):
                context = self.client.fill_context(context, **mapping)[0]
                message_payload.append({"role": "system", "content": context})
        elif context != None and template_name != None:
            mapping.update({"context": context})
            context = self.client.fill_template(template_name=template_name, **mapping)[
                0
            ]
            message_payload.append({"role": "system", "content": context})
        else:
            if template_name != None:
                possibleContent = self.client.fill_template(
                    template_name=template_name, **mapping
                )[0]
                if possibleContent != None:
                    message_payload.append(
                        {"role": "system", "content": [{"text": possibleContent}] }
                    )

        # if history was added, then add it to the payload. Currently history is being like OpenAI prompts
        if history is not None:
            message_payload.extend(history)

        # check if images are in the fill args
        if IMAGE_ENCODED in fill_variables:
            # add the new question to the payload
            if question != None and len(question) > 0:
                image_payload = []
                image_payload.append({"type": "text", "text": question})
                image_payload.append({"image": {"format": fill_variables["image_format"], "source": {"bytes": IMAGE_ENCODED}}})
                message_payload.append({"role": "user", "content": image_payload})
        else:
            # add the new question to the payload
            if question != None and len(question) > 0:
                message_payload.append({"role": "user", "content": [{"text": question}]})

        return message_payload

    def _inference_call(self, prefix: str, stream:bool, **kwargs) -> str:
        final_query = ""
        
        if stream == None:
            stream=True

        # make the call for the response - if stream, it will give right away
        if stream:
            response = self.botoClient.converse_stream(
                modelId=self.modelId,
                **kwargs
            )
            for event in response["stream"]:
                if "contentBlockDelta" in event:
                    text = event["contentBlockDelta"]["delta"]["text"]
                    if text != None:
                        final_query += text
                        print(prefix + text, end="")
            headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
            outputTokens = int(headers.get("x-amzn-bedrock-output-token-count", 0))
#            outputTokens=outputResponse["stream"]["metadata"]["usage"]["outputTokens"]
            if outputTokens==0:
                outputTokens=self.tokenizer.count_tokens(final_query)
        else:
            response = self.botoClient.converse(
                modelId=self.modelId,
                    **kwargs
            )
            final_query = response["output"]["message"]["content"][0]["text"]
            outputTokens=response["usage"]["outputTokens"]

        return final_query, outputTokens
    
    # TODO: this isn't seeming to work correctly
    def check_token_limits(
        self,
        prompt_payload: List,
        user_max_tokens: Optional[int] = None,
    ) -> Tuple[str, int, AskModelEngineResponse]:
        """
        The purpose of this method is to calculate the number of tokens in the prompt and adjust the max_completion_tokens to fit within the context window.
        Args:
            prompt_payload (List): The prompt in the form of chat history
        Returns:
            Tuple[str, int, AskModelEngineResponse]: The truncated prompt, the adjusted max_completion_tokens, and the model engine response dataclass
        """
        model_engine_response = AskModelEngineResponse()
        warnings = []

        # 1. Get our prompt token count
        num_tokens_in_prompt = len(
            self.tokenizer._get_tokenizer(self.modelId).encode(
                self.tokenizer.format_with_chat_template(prompt_payload)
            )
        )

        # 2. Get model limits
        model_limits = self.tokenizer.get_model_limits(self.modelId)
        context_window = model_limits["context_window"]
        max_completion_tokens = model_limits["max_completion_tokens"]
        # If the user provides a token limit for completions we can honor it as long as it is less than the model limit
        if user_max_tokens is not None and user_max_tokens < max_completion_tokens:
            max_completion_tokens = user_max_tokens

        # 3. Define safety margins.. I need this for discrepancy between token counts and actual text length
        SAFETY_PERCENTAGE = 0.01  # 1% for token count safety
        TRUNCATION_THRESHOLD = 0.9  # 90% for truncation decisions

        safety_margin = int(context_window * SAFETY_PERCENTAGE)
        safe_prompt_tokens = num_tokens_in_prompt + safety_margin

        # 4. Check if we need to truncate
        if safe_prompt_tokens > (context_window * TRUNCATION_THRESHOLD):
            token_counter = 0
            truncation_limit = int(context_window * TRUNCATION_THRESHOLD)

            for i, message in enumerate(prompt_payload):
                message_tokens = self.tokenizer.count_tokens(message)
                next_count = token_counter + message_tokens

                if next_count > truncation_limit:
                    # Calculate safe tokens for this message
                    available_tokens = truncation_limit - token_counter
                    if available_tokens > 0:
                        # Truncate this message
                        tokens = self.tokenizer.get_tokens(message["content"])
                        tokens = tokens[:available_tokens]
                        prompt_payload[i]["content"] = "".join(tokens)
                        prompt_payload = prompt_payload[: i + 1]
                    else:
                        # No room for this message
                        prompt_payload = prompt_payload[:i]

                    warnings.append("Prompt was truncated to fit within context window")

                    # Recalculate prompt tokens after truncation
                    num_tokens_in_prompt = len(
                        self.tokenizer._get_tokenizer(self.modelId).encode(
                            self.tokenizer.format_with_chat_template(prompt_payload)
                        )
                    )
                    safe_prompt_tokens = num_tokens_in_prompt + safety_margin
                    break

                token_counter = next_count

        # 5. Calculate available context and final tokens
        available_context = context_window - safe_prompt_tokens
        final_max_tokens = min(available_context, max_completion_tokens)
        final_max_tokens = max(0, final_max_tokens)

        model_engine_response.prompt_tokens = num_tokens_in_prompt
        if len(warnings) > 0:
            model_engine_response.warning = "\\n\\n".join(warnings)

        return prompt_payload, int(final_max_tokens), model_engine_response
