---
sidebar_position: 4
---

# Restore Old Images with Transformers

In this example, we will demonstrate how to use the [SwinIR](https://github.com/JingyunLiang/SwinIR) library and fal-serverless to restore images. SwinIR is an image restoration library that uses a Swin Transformer to restore images. The [Swin Transformer](https://arxiv.org/abs/2103.14030) is a type of neural network architecture that is designed for processing images. The Swin Transformer is similar to the popular Vision Transformer (ViT) architecture, but it uses a hierarchical structure that allows it to process images more efficiently. SwinIR uses a pre-trained Swin Transformer to restore images.

### Step 1: Install fal-serverless and authenticate

```bash
pip install fal-serverless
fal-serverless auth login
```

[More info on authentication](/category/authentication).

### Step 2: Import fal_serverless and define requirements

In a new python file, `ir.py`, import fal_serverless and define a requirements list:

```python
from fal_serverless import isolated, cached

requirements = [
    "timm==0.6.*",
    "numpy==1.24.*",
    "torch==1.13.*",
    "opencv-python-headless==4.7.*",
    "Pillow==9.4.*",]
```

### Step 3: Define cached functions

Next, we define two functions that will be cached using the [@cached decorator](../isolated_functions/cached_function).

```python
@cached
def download_model():
    import os
    model_path = "/data/models/swinir/003_realSR_BSRGAN_DFO_s64w8_SwinIR-M_x4_GAN.pth"
    if not os.path.exists(model_path):
        print("Downloading SwinIR model.")
        url = "https://github.com/JingyunLiang/SwinIR/releases/download/v0.0/003_realSR_BSRGAN_DFO_s64w8_SwinIR-M_x4_GAN.pth"
        os.system(f"mkdir -p /data/models/swinir && cd /data/models/swinir && wget {url}")
        print("Done.")

@cached
def clone_repo():
    import os
    repo_path = "/data/repos/swinir"
    if not os.path.exists(repo_path):
        print("Cloning SwinIR repository")
        os.system("git clone --depth=1 https://github.com/JingyunLiang/SwinIR /data/repos/swinir")
```

The `download_model` function downloads the SwinIR model if it is not already present in the `/data/models/swinir` directory. The `clone_repo` function clones the SwinIR repository from GitHub if it is not already present in the `/data/repos/swinir` directory.

### Step 4: Define the isolated function

Next, we define the `run` function that will be executed using fal-serverless.

```python
@isolated(requirements=requirements, machine_type="GPU", keep_alive=1800)
def run(data):
    import os
    import sys
    import io
    import uuid
    from PIL import Image

    # Setup
    clone_repo()
    download_model()

    os.chdir('/data/repos/swinir')
    imagedir = str(uuid.uuid4())
    os.system(f'mkdir -p {imagedir}')
    if os.path.exists("results/swinir_real_sr_x4"):
        os.system('rm -rf results/swinir_real_sr_x4/*')
    imagename = str(uuid.uuid4())
    img = Image.open(io.BytesIO(data))
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    basewidth = 256
    wpercent = (basewidth/float(img.size[0]))
    hsize = int((float(img.size[1])*float(wpercent)))
    img = img.resize((basewidth,hsize), Image.ANTIALIAS)
    img.save(f"{imagedir}/0.jpg", "JPEG")
    command = (
        f"python main_test_swinir.py --task real_sr --folder_lq {imagedir} --scale 4 "
        "--model_path /data/models/swinir/003_realSR_BSRGAN_DFO_s64w8_SwinIR-M_x4_GAN.pth"
    )
    os.system(command)
    os.system(f"rm -rf {imagedir}")
    with open('results/swinir_real_sr_x4/0_SwinIR.png', "rb") as f:
        result_data = f.read()
    return result_data
```

In this function, we first call the `clone_repo` and `download_model` functions to ensure that we have the SwinIR repository and model downloaded. We then create a directory for the input image and save the image as a JPEG file. We then execute the SwinIR command to restore the image. Finally, we read the restored image and return it in bytes.

### Step 5: Restore an image

Finally, we try to restore an image:

```python
with open("test_image.png", "rb") as f:
    data = f.read()
    result = run(data)

with open("result.png", "wb") as f:
    f.write(result)
```

Here, we're openning `test_image.png` and passing its bytes to the isolated `run` function. We then save the result in `result.png`.

### Conclusion

This example demonstrates how to use the SwinIR model for image restoration by using fal-serverless. This type of image restoration process can be performed in an isolated and scalable manner, while using minimal local resources.
