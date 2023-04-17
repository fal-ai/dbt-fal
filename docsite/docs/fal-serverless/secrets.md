# Secrets

`fal-serverless` offers a convenient way to manage sensitive information, such as API keys or database credentials, within your isolated functions. The secrets functionality enables you to store and access secrets as environment variables, ensuring that your sensitive data remains protected while being readily available when needed.

## Managing Secrets
### Setting Secrets

To store a secret, use the `fal-serverless secrets set` command followed by the secret name and its corresponding value:

```python
fal-serverless secrets set SECRET_NAME SECRET_VALUE
```

This command securely saves the secret to your fal-serverless account, making it accessible within your isolated functions.

### Listing Secrets

To view a list of all stored secrets, use the fal-serverless secrets list command:

```python
fal-serverless secrets list
```

This command displays a table containing the secret names and metadata, such as the creation and modification dates. Note that the secret values are not shown for security reasons.

### Deleting Secrets

To delete a secret, use the fal-serverless secrets delete command followed by the secret name:

```python
fal-serverless secrets delete SECRET_NAME
```

This command removes the secret from your fal-serverless account, making it inaccessible within your isolated functions.

## Accessing Secrets within Isolated Functions

Secrets can be accessed within isolated functions as environment variables. To do this, simply import the `os` module and use the `os.environ` dictionary to retrieve the secret value by its name:

```python
@isolated()
def my_isolated_function():
    import os
    my_secret_value = os.environ['SECRET_NAME']
```

In this example, the `my_secret_value` variable will contain the value of the secret named `SECRET_NAME`. This allows you to securely use sensitive data within your isolated functions without exposing the data in your code.
