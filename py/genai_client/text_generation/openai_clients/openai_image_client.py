from typing import Dict, List
from ...constants import AskModelEngineResponse


class OpenAiImageClient:

    def __init__(self, client):
        self.client = client

    def ask(
        self,
        openai_messages: Dict[str, List[Dict[str, str]]] = None,
        **kwargs,
    ) -> AskModelEngineResponse:
        # TODO: Implment the edit & varation actions
        image_action = kwargs.pop("image_action", "create")

        prompt = (
            openai_messages.get("input")[-1].get("content", None)
            if len(openai_messages)
            else None
        )
        if prompt is None or not isinstance(prompt, str):
            raise ValueError(
                "The last message must contain a text prompt for image generation."
            )

        kwargs["prompt"] = prompt
        kwargs = self._clean_param_map(kwargs)

        if image_action == "create":
            response = self._create_image(kwargs)
        else:
            raise NotImplementedError(
                f"Image action '{image_action}' is not implemented yet."
            )

        return response

    def _create_image(self, image_config) -> AskModelEngineResponse:
        try:
            response = self.client.client.images.generate(**image_config)
            response_format = image_config.get("response_format", "b64_json")
            if response_format == "url":
                image_data = [image.url for image in response.data]
            else:
                image_data = [image.b64_json for image in response.data]

            if self.client.model_name == "gpt-image-1":
                input_tokens = response.usage.input_tokens
                output_tokens = response.usage.output_tokens
            else:
                # TODO: Calculate tokens for DALL-E 2 and DALL-E 3
                input_tokens = 0
                output_tokens = 0

            # Right now I'm just going to return the first image
            if "images" in image_data:
                image_data = image_data["images"][0]

            model_engine_response = AskModelEngineResponse(
                response=image_data,
                response_tokens=output_tokens,
                prompt_tokens=input_tokens,
                messageType="IMAGE",
            )
            return model_engine_response
        except Exception as e:
            print(f"Error generating image: {e}")
            raise

    def _clean_param_map(self, param_map: Dict) -> Dict:
        params = param_map.copy()
        params.pop("message_json", None)
        params.pop("tools", None)
        params.pop("stream", None)
        return params
