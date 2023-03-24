---
sidebar_position: 7
---

# Reference

This is a reference document for `fal-serverless`

## `isolated` decorator

`isolated` decorator lets you define a function that will be run in isolated environment by fal-serverless. Here is a simple example:

```python
from fal_serverless import isolated

@isolated()
def my_function():
  return "Hello World"
```

`my_function` is now what we call an **isolated function**. The subsequent `my_function` calls, such as `my_function()`, are sent to fal-serverless. This means that `my_function` is not run locally, instead it's contents are sent to fal-serverless, executed there and the result is returned to the local environment.

The `isolated` decorator accepts a number of arguments, that are described below.

### `requirements`

You can provide custom requirements to your isolated functions. These requirement are installed only inside a fal-serverless environment and not in your local Python environment.

```python
from fal_serverless import isolated

requirements = ["pyjokes"]

@isolated(requirements=requirements)
def my_function():
  import pyjokes
  return pyjokes.get_joke()
```

In the above example, it is not necessary to install the `pyjokes` module locally. Providing it in a `requirements` list results in fal-serverless automatically installing it in the fal-serverless environment. Note how `pyjokes` is imported inside the `my_function` definition. This way, the Python interpreter will not look for `pyjokes` in your locally installed modules.

### `machine_type`

Isolated functions can run on different machine types. Here's a list of currently supported machine types:

- `XS`: 0.25 CPU cores, 256MB RAM
- `S`: 0.50 CPU cores, 1GB RAM
- `M`: 2 CPU cores, 8GB RAM
- `L`: 4 CPU cores, 32GB RAM
- `XL`: 8 CPU cores, 128GB RAM
- `GPU-T4`: 4 CPU cores, 26GB RAM, 1 GPU core (T4, 16GB VRAM)
- `GPU`: 8 CPU cores, 64GB RAM, 1 GPU core (A100, 40GB VRAM)

The default machine type is `XS`. Machine type can be specified when defining an isolated function:

```python
from fal_serverless import isolated

requirements = ["pyjokes"]

@isolated(requirements=requirements, machine_type="M")
def my_function():
  import pyjokes
  return pyjokes.get_joke()
```

You can also use an isolated function to define a new one with a different machine type:

```python
my_function_L = my_function.on(machine_type="L")
```

In the above example, `my_function_L` is a new isolated function that has the same contents as `my_function`, but it will run on a machine type `L`.

Both functions can be called:

```python
my_function() # executed on machine type `M`
my_function_L() # same as my_function but executed on machine type `L`
```

`my_function` is executed on machine type `M`. And `my_function_L`, which has the same logic as `my_function`, is now executed on machine type `L`.

### Local development

Sometimes you might want to test your isolated functions locally before running them on fal-serverless. For this purpose we provided a special `local` host environment. Taking `my_function` from the previous example:

```python
from fal_serverless import isolated, local

my_function_local = my_function.on(local)
```

`my_function_local` is still an isolated function, but it will not run in fal-serverless. Instead, fal-serverless will create a dedicated isolated Python environment on your local machine and execute `my_function_local` there.

The `.on()` method accepts the same arguments as the `isolated` decorator. So you can define a new isolated function based on `my_function_local` this way:

```python
from fal_serverless import cloud

my_function_cloud = my_function_local.on(cloud, machine_type="M")
```

The resulting `my_function_cloud` is the same as the original `my_function`. It has the same logic and it is executed on a machine type `M`.

Local isolated functions can also be defined with a decorator by providing a `host` argument:

```python
from fal_serverless import isolated, local

requirements = ["pyjokes"]

@isolated(requirements=requirements, host=local)
def my_function_local2():
  import pyjokes
  return pyjokes.get_joke()
```

In this case, `my_function_local` and `my_function_local2` will have exactly the same behavior.

### `credentials`

The credentials argument lets you provide key-based credentials:

```python
from fal_serverless import isolated, CloudKeyCredentials

credentials = CloudKeyCredentials(key_id='your-key-id', key_secret='your-key-secret')

@isolated(credentials=credentials)
def my_function()
    return "hello world"
```

[More information on credentials and authentication](/category/authentication).

### `keep_alive`

The `keep_alive` argument allows for optimization of the function execution process. It lets you specify the number of seconds that the isolated environment should be kept alive after a function execution. This means that subsequent calls to the same function can reuse the existing memory and compute resources if it's still alive, rather than provisioning new infrastructure. This can significantly reduce the time required to start up the runtime and the overall execution time of the isolated function.

By keeping the environment alive, fal-serverless minimizes the time spent on environment setup and initializations. This is especially useful for functions that are frequently called. The keep_alive feature makes it easier to run isolated functions at scale, as it helps to minimize the overhead associated with function execution.

```python
from fal_serverless import isolated

@isolated(keep_alive=20, requirements=["pyjokes"])
def get_joke()
    import pyjokes
    return pyjokes.get_joke()
```

In the example above, isolated environment of `get_joke` will be kept alive for 20 seconds. If `get_joke` is called again within 20 seconds, it will reuse isolated environment **and restart the `keep_alive` timer**.

The default value for keep_alive is 10 seconds.

## `cached` decorator

Functions with `@cached` decorator get their output cached for improved performance. If the same cached function is called in an isolated environment and the environment has been kept alive since the last time the function was called, then the function is not executed and instead, a cached return value is returned. This can significantly reduce the time it takes to execute isolated functions and minimize the resources used.

```python
import time
from fal_serverless import isolated, cached

@cached
def my_cached_function():
    # Simulate a time-consuming calculation
    time.sleep(2)
    return "Hello, World!"

@isolated(keep_alive=10)
def my_isolated_function():
    result = my_cached_function()
    return f"The result is: {result}"

# Call the isolated function multiple times
result1 = my_isolated_function() # Takes more than 2 seconds
result2 = my_isolated_function() # Almost instant

print(result1) # Output: "The result is: Hello, World!"
print(result2) # Output: "The result is: Hello, World!"
```
