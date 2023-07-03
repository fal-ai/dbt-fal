---
sidebar_position: 5
---

# Text-To-Audio with AudioLDM

The `@isolated` decorator lets you to run Python functions in a serverless manner. This means that you can execute your functions in a dedicated environment in the cloud, freeing up local resources and improving performance. In this example, we will build a Text-to-Audio (TTA) function using the `@isolated` decorator.

TTA is a technology that generates audio from a written text prompt. We will use a modified version of [AudioLDM](https://audioldm.github.io/) for this task.

### Step 1: Install fal-serverless and authenticate

```bash
pip install fal-serverless
fal-serverless auth login
```

[More info on authentication](/category/authentication).

### Step 2: Import the `isolated` decorator

Create a file named `tta.py` and import the `isolated` decorator.

```python
from fal_serverless import isolated
```

### Step 3: Define the TTA function

The TTA function will take in a string of text (`prompt`) along with other parameters and convert it into audio.

```python
@isolated(
    requirements=["git+https://github.com/fal-ai/AudioLDM.git@mederka/add-cache-dir-env-var"],
    machine_type="GPU",
    keep_alive=30)
def tta(
        prompt,
        duration=5,
        guidance_scale=2.5,
        random_seed=random.randint(0, 100000),
        n_candidates=3):
    import io
    import os
    import numpy as np
    from scipy.io.wavfile import write

    # Set cache directory to avoid redownloading model weights
    os.environ['TRANSFORMERS_CACHE'] = '/data/hfcache'
    os.environ['AUDIOLDM_CACHE_DIR'] = '/data/audioldm'

    # Import audioldm methods (this also initializes adioldm)
    from audioldm import text_to_audio, build_model

    model = build_model()

    # Run inference
    waveform = text_to_audio(
        model,
        prompt,
        random_seed,
        duration=duration,
        guidance_scale=guidance_scale,
        n_candidate_gen_per_text=int(n_candidates),
    )

    # Store audio in an IO buffer
    buff = io.BytesIO()
    write(buff, 16000, waveform[0][0])

    return output
```

The `tta` function above has the `isolated` decorator, that has three arguments: `requirements`, `machine_type` and `keep_alive`. The `requirements` argument sets our modification of AudioLDM as a package dependency. `machine_type` specifies that the function should run [on a `GPU` machine](../scaling/machine_types), and `keep_alive` makes sure that the target environment [stays alive for 30 seconds](../isolated_functions/keep_alive) after the function execution is complete. So if `tta` is called again within 30 seconds, the target environment is reused.

Inside the `tta` function definition, we use the `text_to_audio` method from `audioldm` to run inference. `text_to_audio` accepts a set of arguments that can be tuned for better results.

### Step 4: Call the TTA function and save the result

```python
res = tta("A hammer hitting a wooden surface.")

with open("result.wav", mode="wb") as f:
    f.write(res.getbuffer())
```

As a result, the audio is saved in a `result.wav` file.

And that's it! You now have a TTA function using the `@isolated` decorator. The function will run in a dedicated environment with all the necessary dependencies already installed.

To demonstrate this function, we have built a [Streamlit app](https://fal-ai-koldstart-tta-example-streamlit-app-zzzjzv.streamlit.app/) for it.
