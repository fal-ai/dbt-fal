---
sidebar_position: 1
---

In 2 short steps, programmatically create a beautiful image using the SDXL model.

**Step 1.** Click on the [Create a Key Pair](https://serverless.fal.ai/dashboard
) button in our dashboard to create a new Key. Make sure to take a note of your Key Secret, as it will not be shown again.

**Step 2.** Use your Key Pair as a Authorization Header to call model APIs provided by fal.

Following is the url for the SDXL model API:

```
https://110602490-sharedsdxl.gateway.alpha.fal.ai
```

You can call the Model API with the language or framework of your choice. Here are some options:

### Python 
```
import requests

headers = {
    "Authorization": "Basic <key_id>:<secret>"
}
response = requests.post(
    "https://110602490-sharedsdxl.gateway.alpha.fal.ai",
    headers=headers,
    json={
        "prompt": "an astronaut in the jungle, cold color pallete with butterflies in the background, highly detailed, 8k",
        "height": 1024,
        "width": 1024,
        "num_inference_steps": 30,
        "guidance_scale": 5,
        "negative_prompt": "blurry",
        "num_images_per_prompt": 1
    },
)
```

### Javascript 
```
import fetch from "node-fetch";

const headers = {
  Authorization: "Basic <key_id>:<secret>",
};

const body = {
  prompt: "an astronaut in the jungle, cold color pallete with butterflies in the background, highly detailed, 8k",
  height: 1024,
  width: 1024,
  num_inference_steps: 30,
  guidance_scale: 5,
  negative_prompt: "blurry",
  num_images_per_prompt: 1
};

fetch("https://110602490-sharedsdxl.gateway.alpha.fal.ai", {
  method: "POST",
  body: JSON.stringify(body),
  headers: headers,
})
  .then((response) => response.json())
  .then((data) => console.log(data))
  .catch((error) => console.error("Error:", error));
```

### cURL
```
curl -X POST https://110602490-sharedsdxl.gateway.alpha.fal.ai \
 -H "Authorization: Basic $FAL_KEY_ID:$FAL_KEY_SECRET" \
 -H "Content-Type: application/json" \
 -d '{"prompt": "an astronaut in the jungle, cold color pallete with butterflies in the background, highly detailed, 8k", "height": 1024, "width": 1024, "num_inference_steps": 30, "guidance_scale": 5, "negative_prompt": "blurry", "num_images_per_prompt": 1}'
```