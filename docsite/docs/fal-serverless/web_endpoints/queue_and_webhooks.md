# Queue and Webhooks

Upon deploying an endpoint to fal-serverless as an HTTP server, such as Flask or FastAPI, several methods exist for interacting with this HTTP server.

Consider the following deployed HTTP server:

```
@isolated(
    requirements=requirements,
    machine_type="GPU-T4",
    keep_alive=300,
    exposed_port=8080,
)
def app():
    from flask import Flask, request
    import uuid

    app = Flask("test")

    @app.route("/test", methods=["POST"])
    def remove():
	    return 200
```

When the application is registered with following command:

```
>> fal-serverless function serve app.py app --alias app

Registered a new revision for function 'app' (revision='39c4e168-414f-49f1-8160-f9f7a958e8cb').
URL: https://123-app.gateway.alpha.fal.ai
```

This application is now ready recieve http requests, exactly like you configured it. The server will respond back when the response is ready.

```
curl -X POST https://123-app.gateway.alpha.fal.ai/test
```

# Queue

You may alternatively choose to add this request to our queue system. Upon doing so, you will promptly receive a request_id. This id can then be used to poll our system periodically until the response is prepared and ready for retrieval.

Utilizing our queue system offers you a more granulated control to handle unexpected surges in traffic. It further provides you with the capability to cancel requests if needed and grants you the observability to monitor your current position within the queue.

To add a request to the queue, simply incorporate the "/fal/queue/submit/" path to the prefix of your URL.

For instance, should you want to use the curl command to submit a request to the aforementioned endpoint and add it to the queue, your command would appear as follows:

```bash
curl -X POST https://123-app.gateway.alpha.fal.ai/fal/queue/submit/test
```

```python
import requests

response = requests.post("123-app.gateway.alpha.fal.ai/fal/queue/submit/test")
data = await response.json()
request_id = data.get("request_id")
```

## Getting the status of request

Once you have the request id you may use this request id to get the status of the request. This endpoint will give you information about your request's status and it's position in the queue.

```python
import requests

response = requests.post("123-app.gateway.alpha.fal.ai/fal/queue/{request_id}/get")
data = await response.json()
queue_position = data.get("queue_position") // 5
queue_position = data.get("status") // IN_PROGRESS
```

## Cancelling a Request

If your request is still in the queue and not already being processed you may cancel it.

```python
import requests

response = requests.post("123-app.gateway.alpha.fal.ai/fal/queue/{request_id}/cancel")
```

# Webhooks

Webhooks work in tandem with the queue system explained above, it is another way to interact with our queue. By providing us a webook endpoint you get notified when the request is done as opposed to polling it.

Here is how this works in practice, it is very similar to submitting something to the queue but we require you to pass an extra fal_webhook query parameter.

```bash
curl -X POST https://123-app.gateway.alpha.fal.ai/fal/queue/submit/test?fal_webhook=url_that_expects_the_hook
```

```python
import requests

response = requests.post("123-app.gateway.alpha.fal.ai/fal/queue/submit/test", params={"fal_webhook": "url_that_expects_the_hook"})
data = await response.json()
request_id = data.get("request_id")
```

Once the request is done waiting in the queue, the webhook url is called with the response from the application.
