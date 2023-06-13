---
sidebar_position: 5
---

# Web Endpoints

Serving an `@isolated` function exposes the function through a webserver managed by `fal-serverless`.

Simplest way to serve a function is to mark the function you want to serve using the `@isolated` decorator's `serve=True` option and `fal-serverless` cli command.

To serve a function do the following steps:

1. Mark the function you want to serve using the `@isolated` decorator with the `serve=True` option:

```python
@isolated(serve=True)
def call_text(text):
    return text
```

2. Use the `fal-serverless` CLI command with the following syntax:

```
fal-serverless function serve ./path/to/file call_text --alias call

>> Registered a new revision for function 'call'  (revision='21847a72-93e6-4227-ae6f-56bf3a90142d').
>> URL: https://1714827-call.gateway.alpha.fal.ai
```

You'll receive an revision ID in the following format: \`XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX\`. This is the revision id of your function.
Everytime you call the `fal-serverless function serve` command a new revision id will
be generated. We will keep the old revisions around so can still access them.

Serving a function with the `--alias` option will create a url that includes the alias you specified instead of the revision id. If you serve a new revision with the same alias, the url will point to the most recent revision of the function.

Alternatively you can call the `fal-serverless function serve` command without the `--alias` option. In that case `fal-serverless` will create an anonymous function that is only accessible by its revision id.

```
fal-serverless function serve ./path/to/file call_text

Registered anonymous function '37f8658e-b841-4b51-ab1a-92565f3a4c04'.
URL: https://1714827-37f8658e-b841-4b51-ab1a-92565f3a4c04.gateway.alpha.fal.ai
```

### Public URLs

By default each registered function is private. In other words requires the caller to pass a FAL key ID and key secret either in the header or as query params. If you wish to skip this validation you can register your function to be public. A public URL is open to the internet and anyone who has access to the URL will be able to call it.

To expose a public URL set the `--auth` option to `public`:

```
fal-serverless function serve ./path/to/file call_text --auth public
```

## Access Served Function via REST API

To access the served function, make a POST REST API request to the following URL:

```
https://<userid>-<alias>.gateway.alpha.fal.ai
```

Replace `<userid>` with your user ID (e.g., `123` if your github id is `github|123`) and `<alias>` with the alias or the revision ID you received earlier. Additionally, pass your FAL key ID and key secret as headers in the request. You can generate keys by following the instructions [here](https://docs.fal.ai/fal-serverless/authentication/env_var).

Here's an example of a cURL request to call the served function:

```bash
curl -X POST "https://123-d9ff88a9-6ae3-45cf-ab67-022e33e4418e.gateway.alpha.fal.ai" -H "Content-Type: application/json" -H "X-Fal-Key-Id:xxxx" -H "X-Fal-Key-Secret:xxxx" -d '{"str":"str to be returned"}'
```

## Expose Function Using Python Web Framework

You can also expose your function using a Python web framework, such as Flask or Fast API. To do so, provide an `exposed_port` in the `@isolated` decorator instead of `serve`. This option gives you more flexibility to decide on which web protocol to use, which ports to expose and other details.

Here's an example using Flask:

```python
@isolated(requirements=["flask"], exposed_port=8080)
def flask_app():
    from flask import Flask, jsonify, request

    app = Flask(__name__)

    @app.route("/")
    def call_str(str):
        return jsonify({"result": str})

    app.run(host="0.0.0.0", port=8080)

```

In this example, the Flask app is exposed on port 8080 and returns the input string as a JSON response.
