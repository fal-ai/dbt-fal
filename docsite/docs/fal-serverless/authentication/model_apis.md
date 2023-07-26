# Authenticate with Model APIs

If you are authenticating with a Model API, you can create a key pair by clicking the [Create a Key Pair](https://serverless.fal.ai/dashboard
) button in our dashboard. Make sure to take a note of your Key Secret, as it will not be shown again.

Once you have the key pair, pass it as an Authentication header to use the Model Api of your choice. 

### cURL
```
curl -X POST https://110602490-sharedsdxl.gateway.alpha.fal.ai \
 -H "Authorization: Basic $FAL_KEY_ID:$FAL_KEY_SECRET" \
 -H "Content-Type: application/json" \
 -d '{"prompt": "an astronaut in the jungle, cold color pallete with butterflies in the background, highly detailed, 8k", "height": 1024, "width": 1024, "num_inference_steps": 30, "guidance_scale": 5, "negative_prompt": "blurry", "num_images_per_prompt": 1}'
```