from google.cloud import aiplatform

# import vertexai
from vertexai.language_models import TextGenerationModel
from google.oauth2 import service_account
import json
from .base_client import BaseClient
from string import Template
import logging
from ..tokenizers import HuggingfaceTokenizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VertexClient(BaseClient):
    def __init__(
        self,
        template=None,
        model_name="text-bison",
        service_account_key_file=None,
        region=None,
        project=None,
        template_name=None,
        **kwargs,
    ):
        super().__init__(
            template = template, 
            template_name = template_name
        )
        self.kwargs = kwargs
        self.model_name = model_name
        self.region = region
        self.project=project
        self.service_account_key_file = service_account_key_file
        
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

    def _init_client(self):
        service_account_info = json.load(open(self.service_account_key_file))
        saCredentials = service_account.Credentials.from_service_account_info(
           service_account_info
        )

        aiplatform.init(
            project=self.project,
            location=self.region,
            credentials=saCredentials,
        )

    def ask(
        self,
        question=None,
        context=None,
        template_name=None,
        history=None,
        max_new_tokens=500,
        temperature=None,
        top_p=None,
        top_k=None,
        prefix="",
        **kwargs,
    ):
        self._init_client()
        final_response = ""  # Initialize final_query once
        output_payload = {}
        
        try:
            message_payload = []

            # Common variable assignment for both chat-completion and completion
            mapping = {"question": question} | kwargs

            # Check if "full_prompt" is specified in kwargs and is set to False
            if (
                "full_prompt" not in kwargs.keys()
                or kwargs.get("full_prompt", False) is False
            ):
                content = []

                # Assert that the question is not None
                if question is None:
                    return "Please ask me a question"

                if template_name is None:
                    template_name = self.template_name

                if history:
                    # Iterate over history only if it's not None
                    for statement in history:
                        content.append(statement["role"])
                        content.append(":")
                        content.append(statement["content"])
                        content.append("\n")

                # Attempt to pull in the context
                sub_occurred = False
                mapping = {"question": question} | kwargs

                if context is not None and template_name is None:
                    # Case 1: The user provided context string in the ask method. We need to check if this string might be a placeholder
                    # where the user wants to fill the context provided as a template
                    if isinstance(context, str):
                        context, sub_occurred = self.fill_context(context, **mapping)
                        content = [context, "\n"]
                elif context is not None and template_name is not None:
                    # Case 2: The user provided context string in the ask method. This context is intended to be string substituted
                    # into a template.
                    # String substitution occurs if either 'question' or 'context' is substituted
                    mapping.update({"context": context})
                    context, sub_occurred = self.fill_template(
                        template_name=template_name, **mapping
                    )
                    content = [context, "\n"]
                else:
                    # Case 3: There was no context provided, however, the user might want to fill the question into a pre-defined template
                    if template_name is not None:
                        possibleContent, sub_occurred = self.fill_template(
                            template_name=template_name, **mapping
                        )
                        if possibleContent is not None:
                            content = [possibleContent, "\n"]

                # Here we need to check if substitution occurred; this determines how we might append history
                if sub_occurred is False:
                    # if substitution did not occur, they are only passing a context string like 'You are a helpful Assistant'
                    # Therefore, we want to append the history and question/prompt directly after
                    if history:
                        # Add history if it's provided
                        for statement in history:
                            content.append(statement["role"])
                            content.append(":")
                            content.append(statement["content"])
                            content.append("\n")

                    # Append the user-asked question to the content
                    content.append(question)
                    # print(content)
                    text_generation_model = TextGenerationModel.from_pretrained(
                        self.model_name
                    )
                    parameters = {
                        "temperature": temperature,
                        "max_output_tokens": max_new_tokens,
                        "top_p": top_p,
                        "top_k": top_k,
                    }
                    
                    prompt="".join(content)
                    output_payload['numberOfTokensInPrompt'] = self.tokenizer.count_tokens(prompt)
                    responses = text_generation_model.predict_streaming(
                        prompt=prompt, **parameters
                    )
                    for response in responses:
                        chunk = response.text
                        print(prefix+chunk,end='')
                        final_response += chunk
                    
                    output_payload['numberOfTokensInResponse'] = self.tokenizer.count_tokens(final_response)
                    output_payload['response'] = final_response
                    return output_payload
                else:
                    # Currently, there is no template where only the context is substituted. At that point, they should pass the context in as an argument.
                    # Therefore, we assume that the question has been substituted and it needs to go last in the prompt string.
                    # As a result, we place the history first
                    hist_content = []
                    if history:
                        for statement in history:
                            hist_content.append(statement["role"])
                            hist_content.append(":")
                            hist_content.append(statement["content"])
                            hist_content.append("\n")
                    content = hist_content + content

                    text_generation_model = TextGenerationModel.from_pretrained(
                        self.model_name
                    )
                    parameters = {
                        "temperature": temperature,
                        "max_output_tokens": max_new_tokens,
                        "top_p": top_p,
                        "top_k": top_k,
                    }
                    
                    output_payload['numberOfTokensInPrompt'] = self.tokenizer.count_tokens(msg_content)
                    responses = text_generation_model.predict_streaming(
                        prompt=msg_content,
                        **parameters,
                    )
                    stream = responses.get("body")
                    final_response = ""
                    if stream is not None:
                        for event in stream:
                            chunk = event.get("chunk")
                            if chunk:
                                partial = json.loads(chunk.get("bytes").decode()).get(
                                    "completion"
                                )
                                print(prefix + partial, end="")
                                final_response += partial
                    
                    output_payload['numberOfTokensInResponse'] = self.tokenizer.count_tokens(final_response)
                    output_payload['response'] = final_response
                    return output_payload

            else:
                full_prompt = kwargs.pop("full_prompt")
                # Default the template name based on the model
                if template_name is None and context is None:
                    template_name = f"{self.model_name}.default.nocontext"
                elif context is not None:
                    template_name = f"{self.model_name}.default.context"

                # Generate the prompt content
                if context is not None:
                    mapping = {"question": question, "context": context}
                # Merge kwargs
                mapping = mapping | kwargs

                prompt_content_tuple = super().fill_template(
                    template_name=template_name, **mapping
                )

                msg_content = prompt_content_tuple[0]

                if msg_content is None:
                    msg_content = question

                # Add history if one is provided
                if history:
                    msg_content = f"{prompt_content} {history}"

                final_query = question + " "
                prompt_content = msg_content
                print(prefix + final_query, end="")

                try:
                    text_generation_model = TextGenerationModel.from_pretrained(
                        self.model_name
                    )
                    parameters = {
                        "temperature": temperature,
                        "max_output_tokens": max_new_tokens,
                        "top_p": top_p,
                        "top_k": top_k,
                    }
                    output_payload['numberOfTokensInPrompt'] = self.tokenizer.count_tokens(msg_content)
                    responses = text_generation_model.predict_streaming(
                        prompt=msg_content,
                        **parameters,
                    )

                    final_response = ""
                    for response in responses:
                        final_response = final_response + response.text
                        
                    output_payload['numberOfTokensInResponse'] = self.tokenizer.count_tokens(final_response)
                    output_payload['response'] = final_response
                    return output_payload
                except Exception as e:
                    logger.error(f"Error while making request to Bedrock: {e}")
                    raise
        except Exception as e:
            print(str(e))
            logger.error(f"Error while making request to Bedrock: {e}")
            raise
        return final_response
