import boto3
import json
from genai_client.client_resources.gaas_client_base import BaseClient
from string import Template
import logging

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
        super().__init__(template=template)
        self.kwargs = kwargs
        self.modelId = modelId
        self.instance_history = []
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.available_models = []
        self.chat_type = "chat-completion"
        self.service_name = service_name
        if "chat_type" in kwargs.keys():
            self.chat_type = kwargs.pop("chat_type")
        self.template_name = template_name

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
                region_name="us-east-1"
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

        try:
            message_payload = []

            # Common variable assignment for both chat-completion and completion
            mapping = {"question": question} | kwargs

            if self.chat_type == "chat-completion":
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
                    return final_response
            # elif self.chat_type == "completion":
            #     # Retain the context part for "completion" chat type here.
            #     prompt = ""

            #     # Default the template name based on model
            #     if template_name is None and context is None:
            #         template_name = f"{self.model_name}.default.nocontext"
            #     elif context is not None:
            #         template_name = f"{self.model_name}.default.context"

            #     # Generate the prompt
            #     if context is not None:
            #         mapping = {"question": question, "context": context}
            #     # Merge kwargs
            #     mapping = mapping | kwargs

            #     prompt = super().fill_template(template_name=template_name, **mapping)

            #     if prompt is None:
            #         prompt = question

            #     # Add history if one is provided
            #     if history is not None:
            #         prompt = f"{prompt} {history}"

            #     final_query = question + " "

            #     # Print statement for debugging can be removed in production
            #     # print(prefix + final_query, end="")
            #     body = self.create_json_body(prompt,max_new_tokens, temperature,top_p)
            #     # body = json.dumps(
            #     #     {
            #     #         "prompt": prompt,
            #     #         "max_tokens_to_sample": max_new_tokens,
            #     #         "temperature": temperature,
            #     #         "top_p": top_p,
            #     #     }
            #     # )
            #     accept = 'application/json'
            #     contentType = 'application/json'

            #     response = client.invoke_model_with_response_stream(
            #         modelId=self.modelId, body=body,
            #          accept=accept, contentType=contentType
            #     )

            #     stream = response.get("body")
            #     if stream:
            #         for event in stream:
            #             chunk = event.get("chunk")
            #             if chunk:
            #                 final_query = json.loads(chunk.get("bytes").decode())
            #                 print(json.loads(chunk.get("bytes").decode()), end="")
        except Exception as e:
            logger.error(f"Error while making request to Bedrock: {e}")
            raise

        return final_response

