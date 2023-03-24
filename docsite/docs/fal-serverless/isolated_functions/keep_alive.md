---
sidebar_position: 3
---

# Keep-Alive - Reusing functions

The `keep_alive` argument is used with the `@isolated` decorator to specify the number of seconds that a target environment should be kept alive after a function is executed.

The purpose of the `keep_alive` argument is to optimize the performance of isolated functions by re-using environments. By keeping the environment alive for a specified number of seconds after function execution, subsequent function calls can be executed more quickly, since the environment does not need to be created from scratch each time.

The `keep_alive` argument is specified when the `@isolated` decorator is applied to a function. For example:

```python
from fal_serverless import isolated

@isolated(keep_alive=300)
def get_two():
  return 1+1

get_two()
```

In this example, the `@isolated` decorator is applied to the `get_two` function with a `keep_alive` argument set to 300 seconds. This means that the target environment will be kept alive for 300 seconds after the function execution is complete. If the same function is called again within 300 seconds, the target environment will be re-used, and the function will be executed in the same environment as before.

Here's another example:

```python
from fal_serverless import isolated

@isolated(keep_alive=10, requirements=["requests"])
def get_response(url):
    import requests
    response = requests.get(url)
    return response.text

get_response('https://www.example.com') # First call (slower)

get_response('https://www.anotherexample.com') # Second call (faster)
```

The first time `get_response` is called, the target environment is created, and the function is executed. The second time `get_response` is called, the target environment is re-used, since it was kept alive for 10 seconds after the first function call.

Note that the `keep_alive` timer is restarted each time `get_response` is called.

In summary, the `keep_alive` argument is a useful feature of the `@isolated` decorator, allowing you to optimize the performance of isolated functions by re-using environments that are still alive. By controlling the number of seconds that the environment will be kept alive, you can strike a balance between performance and resource utilization.
