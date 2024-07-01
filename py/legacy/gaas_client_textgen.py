
from text_generation import Client
from gaas_client_base import BaseClient
import json


class TextGenClient(BaseClient):
    # template to use
    # prefix
    def __init__(self, template_file=None, endpoint=None, model_name="guanaco", **kwargs):
        assert endpoint is not None
        super().__init__(template_file=template_file)
        self.kwargs = kwargs
        self.template_file = template_file
        self.client = Client(endpoint)
        self.model_name = model_name

    def ask(self, question=None, context=None, template_name=None, history=None, max_new_tokens=100, stop_sequences=["#", ";"], temperature_val=0.01, top_p_val=0.5, **kwargs):
        # convert to map
        prompt = ""
        text = ""
        final_output = {}
        mapping = {"question": question}

        # default the template name based on model
        if template_name is None and context is None:
            template_name = f"{self.model_name}.default.nocontext"
        elif context is not None:
            template_name = f"{self.model_name}.default.context"

        # generate the prompt
        if context is not None:
            mapping = {"question": question, "context": context}
        # merge kwargs
        mapping = mapping | kwargs
        # print(mapping)
        prompt = super().fill_template(template_name=template_name, **mapping)

        if prompt is None:
            prompt = question

        print(prompt)
        # Add history if one is provided
        if history is not None:
            prompt = f"{prompt} {history}"

        for response in self.client.generate_stream(prompt, max_new_tokens=max_new_tokens, stop_sequences=stop_sequences, temperature=temperature_val, top_p=top_p_val, **kwargs):
            # for response in client.generate_stream(compose_prompt_qa(context, question), max_new_tokens=max_new_tokens, stop_sequences=stop_sequences, **kwargs):
            if not response.token.special:
                text += response.token.text
            if response.details is not None:
                detail = response.details
                from text_generation.types import FinishReason
                # print(f"Finished with {detail.finish_reason}")
                if detail.finish_reason != "stop_sequence" and detail.finish_reason != "eos_token":
                    finish_reason = f"... <Unable to complete request, please try by increasing token size from {max_new_tokens}>"
                    final_output.update({"meta": finish_reason})
            # print(f"{text}")
        # print(client.generated_stream.finish_reason)
        print(f"{text}")
        final_output.update({"response": f"{text}"})
        return final_output
