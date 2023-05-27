# Deploying a Custom ControlNet Model Using fal-serverless

fal-serverless is a serverless platform that enables you to run Python functions on cloud infrastructure. In this example, we will demonstrate how to use fal-serverless for deploying a custom ControlNet model.

```python
from __future__ import annotations
from fal_serverless import isolated, cached

from pathlib import Path
import base64
import io


requirements = [
    "controlnet-aux",
    "diffusers",
    "torch",
    "mediapipe",
    "transformers",
    "accelerate",
    "xformers"
]

def image_to_base64(image_path):
    with open(image_path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode("utf-8")
    return encoded_string


def read_image_bytes(file_path):
    with open(file_path, "rb") as file:
        image_bytes = file.read()
    return image_bytes

@cached
def load_model():
    import torch
    from diffusers import StableDiffusionControlNetPipeline, ControlNetModel

    controlnet = ControlNetModel.from_pretrained(
        "lllyasviel/sd-controlnet-canny", torch_dtype=torch.float16
    )
    pipe = StableDiffusionControlNetPipeline.from_pretrained(
        "peterwilli/deliberate-2", controlnet=controlnet, torch_dtype=torch.float16
    )

    pipe = pipe.to("cuda:0")
    pipe.unet.to(memory_format=torch.channels_last)
    pipe.controlnet.to(memory_format=torch.channels_last)
    return pipe


def resize_image(input_image, resolution):
    import cv2
    import numpy as np

    H, W, C = input_image.shape
    H = float(H)
    W = float(W)
    k = float(resolution) / min(H, W)
    H *= k
    W *= k
    H = int(np.round(H / 64.0)) * 64
    W = int(np.round(W / 64.0)) * 64
    img = cv2.resize(
        input_image,
        (W, H),
        interpolation=cv2.INTER_LANCZOS4 if k > 1 else cv2.INTER_AREA,
    )
    return img

def save_image_from_bytes(image_bytes, output_path):
    with open(output_path, "wb") as file:
        file.write(image_bytes)

@isolated(
    requirements=requirements,
    machine_type="GPU",
    keep_alive=30,
)
def generate(
    image_bytes: bytes, prompt: str, num_samples: int, num_steps: int, gcs=False
) -> list[bytes] | None:

    from controlnet_aux import CannyDetector
    from PIL import Image
    import numpy as np
    import uuid
    import os

    pipe = load_model()
    image = Image.open(io.BytesIO(image_bytes))


    canny = CannyDetector()
    init_image = image.convert("RGB")

    init_image = resize_image(np.asarray(init_image), 512)
    detected_map = canny(init_image, 100, 200)
    image = Image.fromarray(detected_map)

    negative_prompt = "longbody, lowres, bad anatomy, bad hands, missing fingers, extra digit, fewer digits, cropped, worst quality, low quality"
    results = pipe(
        prompt=prompt,
        image=image,
        negative_prompt=negative_prompt,
        num_inference_steps=num_steps,
        num_images_per_prompt=num_samples
    ).images

    result_id = uuid.uuid4()
    out_dir = Path(f"/data/cn-results/{result_id}")
    out_dir.mkdir(parents=True, exist_ok=True)


    for i, res in enumerate(results):
        res.save(out_dir / f"res_{i}.png")

    file_names = [
        f for f in os.listdir(out_dir) if os.path.isfile(os.path.join(out_dir, f))
    ]

    list_of_bytes = [read_image_bytes(out_dir / f) for f in file_names]
    return list_of_bytes
```
