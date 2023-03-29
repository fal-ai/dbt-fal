---
sidebar_position: 3
---

# Chat with language models

In this example, we will create a chatbot app using fal-serverless, Streamlit, and [FlexGen](https://github.com/FMInference/FlexGen). The chatbot will respond to user input by generating text using a pre-trained large language model that is handled by the FlexGen library. This example is adapted from the [FlexGen chatbot repository](https://github.com/FMInference/FlexGen/tree/main/flexgen/apps).

### Step 1: Install dependencies and authenticate

We only need three packages for this app: `fal-serverless`, `streamlit` and `streamlit-chat`.

```bash
pip install fal-serverless streamlit streamlit-chat
fal-serverless auth login
```

[More info on authentication](/category/authentication).

### Step 2: Define isolated function

Next, in `app.py`, we define the isolated function that will be used to generate responses. Here's the isolated function definition:

```python
from fal_serverless import isolated

@isolated(
    requirements=["git+https://github.com/FMInference/FlexGen.git"],
    machine_type="GPU",
    keep_alive=60,
)
def run(context):
    import argparse
    import os
    from transformers import AutoTokenizer
    from flexgen.flex_opt import (Policy, OptLM, TorchDevice, TorchDisk, TorchMixedDevice,
                                  CompressionConfig, Env, get_opt_config, add_parser_arguments)

    os.environ['TRANSFORMERS_CACHE'] = '/data/hfcache'
    def run_chat(args, context):
        # Initialize environment
        gpu = TorchDevice("cuda:0")
        cpu = TorchDevice("cpu")
        disk = TorchDisk(args.offload_dir)
        env = Env(gpu=gpu, cpu=cpu, disk=disk, mixed=TorchMixedDevice([gpu, cpu, disk]))

        # Offloading policy
        policy = Policy(1, 1,
                        args.percent[0], args.percent[1],
                        args.percent[2], args.percent[3],
                        args.percent[4], args.percent[5],
                        overlap=True, sep_layer=True, pin_weight=args.pin_weight,
                        cpu_cache_compute=False, attn_sparsity=1.0,
                        compress_weight=args.compress_weight,
                        comp_weight_config=CompressionConfig(
                            num_bits=4, group_size=64,
                            group_dim=0, symmetric=False),
                        compress_cache=args.compress_cache,
                        comp_cache_config=CompressionConfig(
                            num_bits=4, group_size=64,
                            group_dim=2, symmetric=False))

        # Model
        tokenizer = AutoTokenizer.from_pretrained("facebook/opt-30b", padding_side="left")
        tokenizer.add_bos_token = False
        stop = tokenizer("\n").input_ids[0]

        opt_config = get_opt_config(args.model)
        model = OptLM(opt_config, env, args.path, policy)
        model.init_all_weights()

        # Chat
        inputs = tokenizer([context])
        output_ids = model.generate(
            inputs.input_ids,
            do_sample=True,
            temperature=0.7,
            max_new_tokens=96,
            stop=stop)
        outputs = tokenizer.batch_decode(output_ids, skip_special_tokens=True)[0]
        try:
            index = outputs.index("\n", len(context))
        except ValueError:
            outputs += "\n"
            index = outputs.index("\n", len(context))

        outputs = outputs[:index + 1]
        return outputs

    parser = argparse.ArgumentParser()
    add_parser_arguments(parser)
    args = parser.parse_args([
        "--model", "facebook/opt-6.7b",
        "--path", "/data/flexgen/weights",
        "--offload-dir", "/data/flexgen/offload",
        "--percent", "100", "0", "100", "0", "100", "0",
        "--pin-weight", "true",

    ])
    return run_chat(args, context=context)
```

This function defines another function `run_chat`, that initializes the environment by setting up the GPU, CPU, and disk devices required for the computation. `run_chat` then loads a pre-trained language model using the `OptLM` class from the FlexGen library. The model is initialized with an offloading policy and the devices defined in the environment. The function generates a response to the input text by passing it through the model using the `generate` method. The `run` function prepares the argument parameters for `run_chat`.

### Step 3: Set up the Stremlit App

In the same file we will create a Streamlit app that will allow users to interact with our chatbot. We will use the `st.session` to hold the session history for us. We will also use [`streamlit_chat`](https://github.com/AI-Yash/st-chat) to display messages.

Here's the code for the Streamlit app:

```python
import streamlit as st
from streamlit_chat import message

if "context" not in st.session_state:
    st.session_state['context'] = (
        "A chat between a curious human and a knowledgeable artificial intelligence assistant.\n"
        "Human: Hello! What can you do?\n"
        "Assistant: As an AI assistant, I can answer questions and chat with you.\n"
        "Human: What is the name of the tallest mountain in the world?\n"
        "Assistant: Everest.\n"
    )

if "output" not in st.session_state:
    st.session_state['output'] = ""

if "chat_history" not in st.session_state:
    st.session_state['chat_history'] = []

st.title("fal-serverless bot")

def get_text():
    input_text = st.text_input("You: ","", key="input")
    return input_text

user_input = get_text()

if user_input:
    st.session_state["chat_history"].append((user_input, True))
    st.session_state['context'] += "Human: " + user_input + "\n"
    st.session_state['output'] = run(st.session_state['context'])
    response = st.session_state['output'][len(st.session_state['context']):]
    st.session_state["chat_history"].append((response[10:], False))
    st.session_state['context'] = st.session_state['output']

if st.session_state["chat_history"]:
    for i in reversed(st.session_state["chat_history"]):
        message(i[0], is_user=i[1], key=f"message-{i}")

```

We initialize session state variables to store the chat history and context. The `get_text` function uses st.text_input to allow users to enter text.

The main app logic generates a response to the user's input using the isolated `run` function, and appends the response and user input to the chat history. The chat history is then displayed using `message` function from `streamlit_chat`.

### Step 4: Running the app

In your terminal, you run:

```bash
streamlit run app.py
```

This will launch the Streamlit app in your default web browser.

### Conclusion

In this example, we demonstrated how to use the FlexGen library and fal-serverless to create a chatbot app that responds to user input by generating text using a pre-trained language model. With these tools, you can create your own chatbots!
