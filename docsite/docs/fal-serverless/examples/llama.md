---
sidebar_position: 3
---

# Run Llama 2 with llama.cpp (OpenAI API Compatible Server)

In this example, we will demonstrate how to use fal-serverless for deploying Llama 2 and serving it through a OpenAI API compatible server with SSE.

# 1. Use already deployed example

If you want to use an already deployed API, here is a public endpoint running on a T4:

https://110602490-llama-server.gateway.alpha.fal.ai/docs

To see this API in action:

```bash
curl -X POST -H "Content-Type: application/json" \
-H "Accept: text/event-stream" \
-H "Authorization: Access-Control-Allow-Origin: *" \
-d '{
  "messages": [
    {
      "role": "user",
      "content": "can you write a happy story"
    }
   ],
   "stream": true,
   "max_tokens": 2000
   }' \
https://110602490-llama-server.gateway.alpha.fal.ai/v1/chat/completions \
```

This should return a streaming response.

# 2. To deploy your own version:

In this example, we will use the conda backend so that we can install CUDA dependencies. First, create the files below:

**llama_cpp_env.yml**

```yaml
name: myenv
channels:
  - conda-forge
  - nvidia/label/cuda-12.0.1
dependencies:
  - cuda-toolkit
  - pip
  - pip:
      - pydantic==1.10.7
      - llama-cpp-python[server]
      - cmake
      - setuptools
```

**llama_cpp.py**

```python
from fal_serverless import isolated, cached

MODEL_URL = "https://huggingface.co/TheBloke/Llama-2-13B-chat-GGML/resolve/main/llama-2-13b-chat.ggmlv3.q4_0.bin"
MODEL_PATH = "/data/models/llama-2-13b-chat.ggmlv3.q4_0.bin"

@isolated(
    kind="conda",
    env_yml="llama_cpp_env.yml",
    machine_type="M",
)
def download_model():
    print("---> This is download_model()")
    import os

    if not os.path.exists("/data/models"):
        os.system("mkdir /data/models")
    if not os.path.exists(MODEL_PATH):
        print("Downloading SAM model.")
        os.system(f"cd /data/models && wget {MODEL_URL}")

@isolated(
    kind="conda",
    env_yml="llama_cpp_env.yml",
    machine_type="GPU-T4",
    exposed_port=8080,
    keep_alive=30
)
def llama_server():
    import uvicorn
    from llama_cpp.server import app

    settings = app.Settings(model=MODEL_PATH, n_gpu_layers=96)

    server = app.create_app(settings=settings)
    uvicorn.run(server, host="0.0.0.0", port=8080)
```

This script has two main functions: one two download the model, and the second one to start the server.

We first need to download the model. You do this by calling the `download_model()` from a Python context. 

We then deploy this as a public endpoint:

```bash
fal-serverless function serve llama_cpp.py llama_server --alias llama-server --auth public
```

This should return a URL, and you can use it like the above. First deploy might take a little bit of time.
