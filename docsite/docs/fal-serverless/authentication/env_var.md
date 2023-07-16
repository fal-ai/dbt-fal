---
sidebar_position: 2
---

# Key-Based Authentication

For automated credential management, fal-serverless provides key-based credentials. In order to generate key-based credentials, you have to use the fal-serverless CLI.

## Generating API Keys

Run the following command to generate a KEY with the scope of your choice.

```bash
fal-serverless key generate --scope ADMIN
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

## Key Scopes

Key scopes provide a way to control the permissions and access levels of different keys within a system. By assigning scopes to keys, you can limit the operations and resources that each key can access. Currently there are only two levels of control `ADMIN` scope and `API` scope. If you are just consuming model APIs, using `API` scope is recommended.

### ADMIN Scope

- Grants full acccess to the SDK.
- Grants full access to CLI operations.
- Grants access to Model API endpoints.

### API Scope

- Grants access to Model API endpoints.

## Using the API credentials

In order to use key-based credentials, you need to set two environment variables `FAL_SERVERLESS_KEY_ID` and `FAL_SERVERLESS_KEY_SECRET`:

```bash
export FAL_SERVERLESS_KEY_ID="your-key-id"
FAL_SERVERLESS_KEY_SECRET="your-key-secret"
```

fal-serverless will automatically detect the above variables.
