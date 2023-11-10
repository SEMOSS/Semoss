import boto3
import json
from .base_client import BaseClient
from string import Template
import logging
from ..tokenizers import HuggingfaceTokenizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BedrockClient(BaseClient):
    def __init__(
        self,
        template=None,
        service_name="bedrock-runtime",
        modelId="anthropic.claude-instant-v1",
        access_key=None,
        secret_key=None,
        region=None,
        template_name=None,
        **kwargs,
    ):
        
        super().__init__(
            template = template, 
            template_name = template_name
        )
        self.kwargs = kwargs
        self.modelId = modelId
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service_name = service_name
        
        # hard code the tokenizer for now
        self.tokenizer = HuggingfaceTokenizer(
            encoder_name = "bert-base-uncased", 
            max_tokens = kwargs.pop(
                'max_tokens', 
                None
            ),
            max_input_tokens = kwargs.pop(
                'max_input_tokens', 
                None
            )
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
                #assuming this is environment auth
                service_name=self.service_name,
                region_name=self.region,
            )
            
    def create_json_body(self,prompt, max_new_tokens, temperature, top_p):
        # Create a dictionary with the desired parameters
        body_dict = {
            "prompt": prompt,
            "max_tokens_to_sample": max_new_tokens,
            "temperature": temperature,
            "top_p": top_p,
        }

        # Filter out parameters with null or empty values
        filtered_body_dict = {key: value for key, value in body_dict.items() if value is not None and value != ""}

        # Convert the filtered dictionary to JSON
        body_json = json.dumps(filtered_body_dict)  # Optional: indent for pretty printing

        return body_json
    
    def ask(
        self,
        question=None,
        context=None,
        template_name=None,
        history=None,
        max_new_tokens=500,
        temperature=None,
        top_p=None,
        prefix="",
        **kwargs,
    ):
        client = self._get_client()
        final_response = ""
        output_payload = {}
        try:
            message_payload = []

            # Common variable assignment for both chat-completion and completion
            mapping = {"question": question} | kwargs


            if "full_prompt" not in kwargs.keys():
                if context and not template_name:
                    if isinstance(context, str):
                        context = self.fill_context(context, **mapping)[0]
                        message_payload.append(
                            {"role": "system", "content": context}
                        )
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
                output_payload['numberOfTokensInPrompt'] = self.tokenizer.count_tokens(prompt_content)
                body = self.create_json_body(prompt_content,max_new_tokens, temperature,top_p)
                # body = json.dumps(
                #     {
                #         "prompt": prompt_content,
                #         "max_tokens_to_sample": max_new_tokens,
                #         "temperature": temperature,
                #         "top_p": top_p,
                #     }
                # )
                accept = 'application/json'
                contentType = 'application/json'
                response = client.invoke_model_with_response_stream(
                    modelId=self.modelId, body=body,
                    accept=accept, contentType=contentType
                )
                stream = response.get("body")
                if stream:
                    for event in stream:
                        chunk = event.get("chunk")
                        if chunk:
                            partial = json.loads(chunk.get("bytes").decode()).get('completion')
                            print(prefix+partial,end='')
                            final_response += partial
                
                output_payload['numberOfTokensInResponse'] = self.tokenizer.count_tokens(final_response)
                output_payload['response'] = final_response
                return output_payload

        except Exception as e:
            logger.error(f"Error while making request to Bedrock: {e}")
            raise

        return final_response

