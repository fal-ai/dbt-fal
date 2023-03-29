---
sidebar_position: 2
---

# Key-Based Authentication

For automated credential management, fal-serverless provides key-based credentials. In order to generate key-based credentials, you have to use the fal-serverless CLI.

## Generating API Keys

```bash
fal-serverless key generate
```

If successful, the following message should be printed out in your terminal:

```
Generated key id and key secret.
This is the only time the secret will be visible.
You will need to generate a new key pair if you lose access to this secret.
KEY_ID='your-key-id'
KEY_SECRET='your-key-secret'
```

You should note the values of `KEY_ID` and `KEY_SECRET`.

## Using the API credentials

In order to use key-based credentials, you need to set two environment variables `FAL_SERVERLESS_KEY_ID` and `FAL_SERVERLESS_KEY_SECRET`:

```bash
export FAL_SERVERLESS_KEY_ID="your-key-id"
FAL_SERVERLESS_KEY_SECRET="your-key-secret"
```

fal-serverless will automatically detect the above variables.
