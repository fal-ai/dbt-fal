---
sidebar_position: 5
---

# Image Generation Using Stable Diffusion
fal-serverless is a serverless platform that enables you to run Python functions on cloud infrastructure. In this example, we will demonstrate how to use fal-serverless for image generation with help from the stable diffusion model.

### Step 1: Install fal-serverless and authenticate

```bash
pip install fal-serverless
fal-serverless auth login
```

[More info on authentication](/category/authentication).


## Step 2: Import required libraries

First, we need to import the necessary libraries and define the requirements for the `@isolated` decorator:

```python
from fal_serverless import isolated
import io

requirements = [
    "accelerate",
    "diffusers[torch]>=0.10",
    "ftfy",
    "torch",
    "torchvision",
    "transformers",
    "triton",
    "safetensors",
    "xformers==0.0.16",
]
```

## Step 3: Define the generate function

Next, we will define the `generate` function, which will be responsible for generating the image using the stable diffusion model:

```python
@isolated(requirements=requirements, machine_type="GPU-T4")
def generate(prompt: str):
    import torch
    import os
    import base64
    from diffusers import StableDiffusionPipeline, DPMSolverMultistepScheduler

    model_id = "runwayml/stable-diffusion-v1-5"
    os.environ['TRANSFORMERS_CACHE'] = '/data/hugging_face_cache'

    pipe = StableDiffusionPipeline.from_pretrained(
        model_id,
        num_inference_steps=20,
        torch_dtype=torch.float16,
        cache_dir="/data/hfcache")
    pipe = pipe.to("cuda")
    pipe.scheduler = DPMSolverMultistepScheduler.from_config(pipe.scheduler.config)

    generator = torch.Generator("cuda")
    image = pipe(prompt, generator=generator).images[0]

    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf
```
We set the package and machine type requirements as `isolated` argument. `generate` function then imports the necessary modules and initializes the stable diffusion pipeline using the specified model ID. It also sets up the Hugging Face transformers cache directory to use fal-serverless persistent [/data directory](../storage_and_persistence). The pipeline is then moved to the GPU and configured with a custom scheduler. Finally, the function generates the image based on the given prompt and returns it as a byte buffer.

## Step 4: Generate the image
Now that we have defined the `generate` function, we can use it to generate an image by passing a prompt:

```python
image_data = generate("Donkey walking on clouds")
```
This will generate an image based on the given prompt "Donkey walking on clouds" and store it in image_data.

## Step 5: Save the generated image
Once we have the generated image data, we can save it to a file:

```python
with open("test.png", "wb") as f:
    f.write(image_data.getvalue())
```
This will save the generated image as `test.png` in the current directory.

## Conclusion
In this example, we demonstrated how to use fal-serverless to generate images using the stable diffusion model. By utilizing fal-serverless infrastructure, we can easily deploy and scale this image generation process without worrying about managing servers or other infrastructure components.
