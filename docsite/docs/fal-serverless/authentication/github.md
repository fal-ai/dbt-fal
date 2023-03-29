---
sidebar_position: 1
---

# Github Authentication

`fal-serverless` uses GitHub authentication by default. This means that in order to use fal-serverless you need to have a [GitHub account](https://github.com/login).

## Logging in

[Installing fal-serverless](/fal-serverless/quickstart) Python library gives access to `fal-serverless CLI`, which you can use for authentication. In your terminal, you can run the following command:

```bash
fal-serverless auth login
```

Follow the instructions on your terminal to confirm your credentials. Once you're done, you should get a success message in your terminal.

Now you're ready to write your first fal-serverless function!

**Note:** that your login credentials are persisted on your local machine and cannot be transfered to another machine. If you want to use fal-serverless on multiple machines, you either need to login on each one or use [key-based credentials](#key_based_credentials).
