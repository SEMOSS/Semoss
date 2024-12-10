import openai
from gaas_client_base import BaseClient


class OpenAiClient(BaseClient):
    # template to use
    # prefix
    #'vicuna-7b-v1.3'
    # openai.api_base = "https://play.semoss.org/fastchat/v1/"
    def __init__(
        self,
        template_file=None,
        endpoint=None,
        model_name="vicuna-7b-v1.3",
        api_key="EMPTY",
        **kwargs,
    ):
        assert endpoint is not None
        super().__init__(template_file=template_file)
        self.kwargs = kwargs
        self.template_file = template_file
        self.model_name = model_name
        self.endpoint = endpoint
        self.api_key = api_key

    def ask(
        self,
        question=None,
        context=None,
        template_name=None,
        history=None,
        page_size=100,
        max_new_tokens=100,
        stop_sequences=["#", ";"],
        temperature_val=0.01,
        top_p_val=0.5,
        **kwargs,
    ):
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

        # Add history if one is provided
        if history is not None:
            prompt = f"{prompt} {history}"

        # forcing the api_key to a dummy value
        if openai.api_key is None:
            openai.api_key = self.api_key
        openai.api_base = self.endpoint

        final_query = ""
        tokens = 0
        finish = False
        while tokens < max_new_tokens and not finish:
            full_prompt = f"{prompt}{final_query}"
            # print(f"Going in prompt is {full_prompt}")
            response = openai.Completion.create(
                model=self.model_name,
                prompt=full_prompt,
                temperature=temperature_val,
                max_tokens=page_size,
                top_p=top_p_val,
                stop=stop_sequences,
                **kwargs,
            )
            # print(response)
            output = response.choices[0].text
            final_query = f"{final_query}{output}"
            tokens = tokens + page_size
            finish = response.choices[0].finish_reason == "stop"
            # print(response.choices[0].finish_reason)
            # give partial output for typing effect
            print(output, end="0")

        return final_query
