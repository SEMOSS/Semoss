import os
import datetime
import torch
from typing import Optional
from diffusers import PixArtAlphaPipeline
from io import BytesIO
import base64
import asyncio
import json
import websockets
import datetime
import os
import base64
import zlib
from genai_client.image_generation.abstract_image_generation_client import (
    AbstractImageGenerationClient,
)


class ImageGenClient(AbstractImageGenerationClient):
    def __init__(
        self,
        model_name: str = "PixArt-alpha/PixArt-XL-2-1024-MS",
        device: str = "cuda:0",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.device = torch.device(device if torch.cuda.is_available() else "cpu")
        self.pipe = PixArtAlphaPipeline.from_pretrained(
            model_name,
            torch_dtype=torch.float16 if self.device.type == "cuda" else torch.float32,
            use_safetensors=True,
        )
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
        seed: Optional[int] = None,
        remote_client: Optional[bool] = False,
    ):
        if remote_client:
            return asyncio.run(
                self.generate_image_remote(
                    prompt,
                    output_dir,
                    file_name,
                    consistency_decoder,
                    negative_prompt,
                    guidance_scale,
                    num_inference_steps,
                    height,
                    width,
                    seed,
                )
            )
        else:
            return self.generate_image_local(
                prompt,
                output_dir,
                file_name,
                consistency_decoder,
                negative_prompt,
                guidance_scale,
                num_inference_steps,
                height,
                width,
                seed,
            )

    async def generate_image_remote(
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
        seed: Optional[int] = None,
    ):
        uri = "ws://localhost:8888"

        try:
            async with websockets.connect(uri, timeout=60) as websocket:
                request = {
                    "prompt": prompt,
                    "consistency_decoder": consistency_decoder,
                    "negative_prompt": negative_prompt,
                    "guidance_scale": guidance_scale,
                    "num_inference_steps": num_inference_steps,
                    "height": height,
                    "width": width,
                    "seed": seed,
                    "file_name": file_name,
                }
                request_data = json.dumps(request)

                print(f"Sending: {request_data}")
                await websocket.send(request_data)

                chunks = []
                total_chunks = None

                while True:
                    try:
                        response = await asyncio.wait_for(
                            websocket.recv(), timeout=300
                        )  # 5-minute timeout
                        print(f"Received: {response}")

                        response_data = json.loads(response)

                        if "error" in response_data:
                            raise Exception(f"Server error: {response_data['error']}")

                        if "total_chunks" in response_data:
                            total_chunks = response_data["total_chunks"]

                        if "chunk_index" in response_data:
                            # chunk_index = response_data["chunk_index"]
                            chunks.append(response_data["data"])

                            if len(chunks) == total_chunks:
                                print("All chunks received. Reconstructing image...")
                                base64_uncompressed = "".join(chunks)
                                print(base64_uncompressed)
                                break

                        elif response_data.get("status") == "processing":
                            print("Still processing...")

                    except asyncio.TimeoutError:
                        print("Timeout waiting for server response")
                        break

                base64_image = base64.b64decode(base64_uncompressed)

                unique_identifier = datetime.datetime.now().strftime(
                    "%Y-%m-%d_%H-%M-%S"
                )
                file_path = os.path.join(
                    output_dir, f"{file_name}-{unique_identifier}.png"
                )

                with open(file_path, "wb") as image_file:
                    image_file.write(base64_image)

                return {
                    "file_path": file_path,
                    "generation_time": response_data.get("generation_time"),
                    "seed": response_data.get("seed"),
                    "prompt": response_data.get("prompt"),
                    "negative_prompt": response_data.get("negative_prompt"),
                    "guidance_scale": response_data.get("guidance_scale"),
                    "num_inference_steps": response_data.get("num_inference_steps"),
                    "height": response_data.get("height"),
                    "width": response_data.get("width"),
                    "model_name": response_data.get("model_name"),
                    "vae_model_name": response_data.get("vae_model_name"),
                    "base64Image": base64_uncompressed,
                }

        except websockets.exceptions.ConnectionClosed as e:
            print(f"Connection closed: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

        return None  # Return None if the process fails

    # async def generate_image_remote(
    #     self,
    #     prompt: str,
    #     output_dir: str,
    #     file_name: str = "generated_image",
    #     consistency_decoder: Optional[bool] = False,
    #     negative_prompt: Optional[str] = None,
    #     guidance_scale: Optional[float] = 7.5,
    #     num_inference_steps: Optional[int] = 50,
    #     height: Optional[int] = 512,
    #     width: Optional[int] = 512,
    #     seed: Optional[int] = None,
    # ):
    #     uri = "ws://localhost:8888"

    #     try:
    #         async with websockets.connect(uri, timeout=60) as websocket:
    #             request = {
    #                 "prompt": prompt,
    #                 "consistency_decoder": consistency_decoder,
    #                 "negative_prompt": negative_prompt,
    #                 "guidance_scale": guidance_scale,
    #                 "num_inference_steps": num_inference_steps,
    #                 "height": height,
    #                 "width": width,
    #                 "seed": seed,
    #                 "file_name": file_name,
    #             }
    #             request_data = json.dumps(request)

    #             print(f"Sending: {request_data}")
    #             await websocket.send(request_data)

    #             chunks = []
    #             total_chunks = None

    #             while True:
    #                 try:
    #                     response = await asyncio.wait_for(
    #                         websocket.recv(), timeout=300
    #                     )  # 5-minute timeout
    #                     print(f"Received: {response}")

    #                     response_data = json.loads(response)

    #                     if "error" in response_data:
    #                         raise Exception(f"Server error: {response_data['error']}")

    #                     # if response_data.get("status") == "complete":
    #                     if "base64Image" in response_data:
    #                         print("Received image data...")
    #                         base64_image = response_data.get("base64Image")
    #                         if base64_image:
    #                             try:
    #                                 compressed_data = base64.b64decode(base64_image)
    #                                 base64_uncompressed = zlib.decompress(
    #                                     compressed_data
    #                                 ).decode("utf-8")
    #                                 # Now use base64_uncompressed
    #                             except (
    #                                 zlib.error,
    #                                 UnicodeDecodeError,
    #                                 base64.binascii.Error,
    #                             ) as e:
    #                                 print(f"Error decompressing image data: {e}")

    #                             unique_identifier = datetime.datetime.now().strftime(
    #                                 "%Y-%m-%d_%H-%M-%S"
    #                             )
    #                             file_path = os.path.join(
    #                                 output_dir, f"{file_name}-{unique_identifier}.png"
    #                             )

    #                             with open(file_path, "wb") as image_file:
    #                                 image_file.write(
    #                                     base64.b64decode(base64_uncompressed)
    #                                 )

    #                         return {
    #                             "file_path": file_path,
    #                             "generation_time": response_data.get("generation_time"),
    #                             "seed": response_data.get("seed"),
    #                             "prompt": response_data.get("prompt"),
    #                             "negative_prompt": response_data.get("negative_prompt"),
    #                             "guidance_scale": response_data.get("guidance_scale"),
    #                             "num_inference_steps": response_data.get(
    #                                 "num_inference_steps"
    #                             ),
    #                             "height": response_data.get("height"),
    #                             "width": response_data.get("width"),
    #                             "model_name": response_data.get("model_name"),
    #                             "vae_model_name": response_data.get("vae_model_name"),
    #                             "base64Image": base64_uncompressed,
    #                         }

    #                     elif response_data.get("status") == "processing":
    #                         print("Still processing...")

    #                 except asyncio.TimeoutError:
    #                     print("Timeout waiting for server response")
    #                     break

    #     except websockets.exceptions.ConnectionClosed as e:
    #         print(f"Connection closed: {e}")
    #     except Exception as e:
    #         print(f"An error occurred: {e}")

    #     return None  # Return None if the process fails

    def generate_image_local(
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
        seed: Optional[int] = None,
    ) -> dict:
        """_summary_
        Generate an image using the PixArtAlpha pipeline on your local machine.
        Args:
            prompt (str): _description_
            output_dir (str): _description_
            file_name (str, optional): _description_. Defaults to "generated_image".
            consistency_decoder (Optional[bool], optional): _description_. Defaults to False.
            negative_prompt (Optional[str], optional): _description_. Defaults to None.
            guidance_scale (Optional[float], optional): _description_. Defaults to 7.5.
            num_inference_steps (Optional[int], optional): _description_. Defaults to 50.
            height (Optional[int], optional): _description_. Defaults to 512.
            width (Optional[int], optional): _description_. Defaults to 512.
            seed (Optional[int], optional): _description_. Defaults to None.
        """

        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)

        # If using DALL-E 3 Consistency Decoder.. This takes a long time..
        # Varitaional Autoencoder (VAE) enhances performance of image generation pipeline
        # by ensuring high quality coherent outputs
        if consistency_decoder:
            from diffusers import ConsistencyDecoderVAE

            self.pipe.vae = ConsistencyDecoderVAE.from_pretrained(
                "openai/consistency-decoder", torch_dtype=torch.float16
            )
            self.pipe.vae.to(self.device)

        # If seed is not provided by user, generate a random seed.. This is normal seed process
        # else we use the seed provided by the user
        if seed is not None and seed > 0:
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
            **{
                k: v.to(self.device) if torch.is_tensor(v) else v
                for k, v in inputs.items()
            }
        )
        end_time = datetime.datetime.now()
        generation_time = (end_time - start_time).total_seconds()

        image = outputs.images[0]
        # Avoid using the same file name for multiple images
        unique_identifier = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_path = os.path.join(output_dir, f"{file_name}-{unique_identifier}.png")

        image.save(file_path)

        # Converting image to base64
        buffered = BytesIO()
        image.save(buffered, format="PNG")
        base64_str = base64.b64encode(buffered.getvalue()).decode()

        if base64_str is None or base64_str == "":
            base64_str = "THERE WAS A PROBLEM"

        response = {
            "base64Image": base64_str,
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
            "vae_model_name": (
                "openai/consistency-decoder" if consistency_decoder else "default"
            ),
        }

        return response
