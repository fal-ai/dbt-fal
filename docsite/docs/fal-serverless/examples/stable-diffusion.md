---
sidebar_position: 1
---

# Generate Images from Text with Stable Diffusion
In this example, we will deploy Stable Diffusion using fal-serverless. As we do that, we will learn about important fal-serverless concepts.

## Step 1: Install fal-serverless and authenticate

```bash
pip install fal-serverless
fal-serverless auth login
```

[More info on authentication](/category/authentication).


## Step 2: Import required libraries

First, we need to define the requirements for our project:

```python
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

Next, we will define the `generate` function, which will be responsible for generating an image using Stable Diffusion:

```python
from fal_serverless import isolated

@isolated(requirements=requirements, machine_type="GPU-T4", keep_alive=30)
def generate(prompt: str):
    import torch
    import os
    import io
    import base64
    from diffusers import StableDiffusionPipeline, DPMSolverMultistepScheduler

    model_id = "runwayml/stable-diffusion-v1-5"
    os.environ['TRANSFORMERS_CACHE'] = '/data/hugging_face_cache'

    pipe = StableDiffusionPipeline.from_pretrained(
        model_id,
        torch_dtype=torch.float16,
        cache_dir=os.environ['TRANSFORMERS_CACHE'])
    pipe = pipe.to("cuda")
    pipe.scheduler = DPMSolverMultistepScheduler.from_config(pipe.scheduler.config)

    image = pipe(prompt, num_inference_steps=20).images[0]

    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf
```

The `@isolated` decorator is the most important building block in fal-serverless. It lets you run any Python function in the cloud instantly on many types of GPUs. In this example, the decorator accepts a `requirements` argument which defines the libraries needed to run the function, a `machine_type` argument that specifies the machine that we want to run this function on and a `keep_alive` argument that specifies the number of seconds to keep the underlying machine alive in case there are no other function calls.

## Step 4: Generate the image
Now that we defined the `generate` function, we can use it to generate an image by passing a prompt. In fal-serverless, you call an `@isolated` function just as if it is a local Python function.

```python
image_data = generate("Donkey walking on clouds")
```

This will generate an image based on the given prompt "Donkey walking on clouds" and store it in image_data.

To save this image locally:
```python
with open("test.png", "wb") as f:
    f.write(image_data.getvalue())
```

## Step 5: Make it faster with @cached
You may notice that we are loading the model to GPU VRAM every time we call the generate function. We will now introduce another building block in fal-serverless: the `@cached` decorator. This decorator lets you keep expensive operations in memory. By caching the model, we can get improved performance. Our code now looks like:

```python
from fal_serverless import isolated, cached

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

@cached
def model():
    import torch
    import os
    import io
    import base64
    from diffusers import StableDiffusionPipeline, DPMSolverMultistepScheduler

    model_id = "runwayml/stable-diffusion-v1-5"
    os.environ['TRANSFORMERS_CACHE'] = '/data/hugging_face_cache'

    pipe = StableDiffusionPipeline.from_pretrained(
        model_id,
        torch_dtype=torch.float16,
        cache_dir=os.environ['TRANSFORMERS_CACHE'])
    pipe = pipe.to("cuda")
    pipe.scheduler = DPMSolverMultistepScheduler.from_config(pipe.scheduler.config)
    return pipe

@isolated(requirements=requirements, machine_type="GPU", keep_alive=30)
def generate(prompt: str):
    import io

    pipe = model()
    image = pipe(prompt, num_inference_steps=50).images[0]

    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf

image_data = generate("astronaut riding a horse")

with open("test.png", "wb") as f:
    f.write(image_data.getvalue())
```
