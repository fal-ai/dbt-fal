```
from fal_serverless import isolated, cached

requirements = [
"transformers",
"sentencepiece",
"accelerate",
"fal_serverless",
"torch==2.0",
"numpy",
"tokenizers>=0.12.1",
]

@cached
def load_model():
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

    print(torch.__version__)

    tokenizer = AutoTokenizer.from_pretrained(
        "TheBloke/vicuna-13B-1.1-HF", use_fast=False
    )

    model = AutoModelForCausalLM.from_pretrained(
        "TheBloke/vicuna-13B-1.1-HF", low_cpu_mem_usage=True, torch_dtype=torch.float16
    )

    model.to("cuda")

    return tokenizer, model

@isolated(requirements=requirements, machine_type="GPU", keep_alive=60)
def llm(prompt):
import os
import torch

    print(torch.__version__)

    os.environ["TRANSFORMERS_CACHE"] = "/data/hfcache"

    with torch.inference_mode():
        tokenizer, model = load_model()
        input_ids = tokenizer([prompt]).input_ids

        output_ids = model.generate(
            torch.as_tensor(input_ids).cuda(),
            do_sample=True,
            temperature=0.7,
            max_new_tokens=512,
        )
        output_ids = output_ids[0][len(input_ids[0]) :]
        outputs = tokenizer.decode(
            output_ids, skip_special_tokens=True, spaces_between_special_tokens=False
        )

        return outputs

print(llm("give me a itenary for a trip to japan"))
```
