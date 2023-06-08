# Deploying an Open Source LLM Using fal-serverless

fal-serverless is a serverless platform that enables you to run Python functions on cloud infrastructure. In this example, we will demonstrate how to use fal-serverless for deploying the open source LLM model "TheBloke/vicuna-13B-1.1-HF" and serving it in a streaming fashion using FastAPI.

```python
from fal_serverless import isolated, cached

requirements = [
    "transformers",
    "sentencepiece",
    "accelerate",
    "torch==2.0",
    "numpy",
    "tokenizers>=0.12.1",
    "fastapi",
    "uvicorn",
    "supabase"
]

@cached
def load_model():
    import os
    cache_dir = "/data/hfcache"
    os.environ["TRANSFORMERS_CACHE"] = cache_dir

    import torch
    from transformers import AutoTokenizer, AutoModelForCausalLM

    print(torch.__version__)
    tokenizer = AutoTokenizer.from_pretrained(
        "TheBloke/vicuna-13B-1.1-HF", use_fast=False, cache_dir=cache_dir
    )
    model = AutoModelForCausalLM.from_pretrained(
        "TheBloke/vicuna-13B-1.1-HF",
        low_cpu_mem_usage=True,
        torch_dtype=torch.float16,
        cache_dir=cache_dir,
    )
    model.to("cuda")
    return tokenizer, model

@isolated(requirements=requirements, machine_type="GPU", keep_alive=30, exposed_port=8080)
def chat_app():
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import StreamingResponse
    from pydantic import BaseModel
    from transformers import TextIteratorStreamer
    import os
    import torch
    import uvicorn
    import uuid
    import time

    from supabase import create_client

    app = FastAPI()

    class ChatInput(BaseModel):
        prompt: str

    async def __process_prompt(prompt: str):
        print(torch.__version__)
        os.environ["TRANSFORMERS_CACHE"] = "/data/hfcache"
        with torch.inference_mode():
            tokenizer, model = load_model()
            streamer = TextIteratorStreamer(
                tokenizer,
                skip_prompt=True,
                # Decoder's parameters
                skip_special_tokens=True,
                spaces_between_special_tokens=False
            )
            input_ids = tokenizer([prompt]).input_ids
            _ = model.generate(
                torch.as_tensor(input_ids).cuda(),
                do_sample=True,
                temperature=0.7,
                max_new_tokens=512,
                streamer=streamer,
            )
            for content in streamer:
                yield content

    @app.post("/chat")
    async def process_prompt(input: ChatInput):
        resp = __process_prompt(input.prompt)

        return StreamingResponse(
            resp,
            media_type="text/plain",
        )

    uvicorn.run(app, host="0.0.0.0", port=8080)
```

As you may have noticed, this example also includes the webserver code to serve the model. We wanted to include it to demonstrate how to stream the LMM results. To deploy the `app` function as a web server with fal-serverless run the following command:

```
fal-serverless function serve /path_to_file.py chat_app
```
