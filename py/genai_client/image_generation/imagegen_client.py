import os
import datetime
import torch
from typing import Optional
from diffusers import PixArtAlphaPipeline
from .abstract_image_generation_client import AbstractImageGenerationClient


class ImageGenClient(AbstractImageGenerationClient):
    def __init__(self, model_name: str = "PixArt-alpha/PixArt-XL-2-1024-MS", device: str = "cuda:0", **kwargs):
        super().__init__(**kwargs)
        self.device = torch.device(
            device if torch.cuda.is_available() else "cpu")
        self.pipe = PixArtAlphaPipeline.from_pretrained(
            model_name, torch_dtype=torch.float16, use_safetensors=True)
        self.pipe.to(self.device)
        # Enable memory optimizations
        self.pipe.enable_model_cpu_offload()

    def generate_image(
        self,
        prompt: str,
        output_dir: str,
        file_name: str = "generated_image",
        consistency_decoder: Optional[bool] = False,
        negative_prompt: Optional[str] = None,
        guidance_scale: Optional[float] = 7.5,
        num_inference_steps: Optional[int] = 50,
        height: Optional[int] = 512,
        width: Optional[int] = 512,
        seed: Optional[int] = None
    ) -> dict:

        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)

        # If using DALL-E 3 Consistency Decoder.. This takes a long time..
        # Varitaional Autoencoder (VAE) enhances performance of image generation pipeline
        # by ensuring high quality coherent outputs
        if consistency_decoder:
            from diffusers import ConsistencyDecoderVAE
            self.pipe.vae = ConsistencyDecoderVAE.from_pretrained(
                "openai/consistency-decoder", torch_dtype=torch.float16)
            self.pipe.vae.to(self.device)

        # If seed is not provided by user, generate a random seed.. This is normal seed process
        # else we use the seed provided by the user
        if seed is not None:
            generator = torch.Generator(device=self.device).manual_seed(seed)
        else:
            generator = torch.Generator(device=self.device)
            seed = generator.seed()

        # Move inputs to the same device as model
        inputs = {
            "prompt": prompt,
            "negative_prompt": negative_prompt,
            "guidance_scale": guidance_scale,
            "num_inference_steps": int(num_inference_steps),
            "height": int(height),
            "width": int(width),
            "generator": generator,
        }

        self.pipe.enable_attention_slicing()

        start_time = datetime.datetime.now()
        outputs = self.pipe(
            **{k: v.to(self.device) if torch.is_tensor(v) else v for k, v in inputs.items()})
        end_time = datetime.datetime.now()
        generation_time = (end_time - start_time).total_seconds()

        image = outputs.images[0]
        # Avoid using the same file name for multiple images
        unique_identifier = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        file_path = os.path.join(
            output_dir, f"{file_name}-{unique_identifier}.png")

        image.save(file_path)

        response = {
            "file_path": file_path,
            "generation_time": int(generation_time),
            "seed": str(seed),
            "prompt": prompt,
            "negative_prompt": negative_prompt,
            "guidance_scale": guidance_scale,
            "num_inference_steps": int(num_inference_steps),
            "height": int(height),
            "width": int(width),
            "model_name": self.pipe.name_or_path,
            "vae_model_name": "openai/consistency-decoder" if consistency_decoder else "default",
        }

        return response
