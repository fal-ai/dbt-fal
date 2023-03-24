---
sidebar_position: 1
---

# Overview

Isolated Functions are Python functions that use the `@isolated` decorator.

With `@isolated` you can run Python function in a serverless manner. This is accomplished by creating a dedicated environment in the cloud when an isolated function is called. The environment is then destroyed to save resources. This allows you to run functions in an isolated and scalable manner, freeing up local resources and improving performance.

The `@isolated` decorator accepts several arguments, including [`keep_alive`](./keep_alive), [`requirements`](managing_environments), [`machine_type`](../scaling/machine_types) and more. The `keep_alive` argument determines the number of seconds that the target environment should be kept alive after function execution is complete. The `requirements` argument is a list of packages to be installed in the target environment. The `machine_type` argument lets you specify the size of a machine, on which the isolated function is executed.

You can also define [cached functions](./cached_function) for time consuming operations and improve performance of your isolated functions.

By using `@isolated`, you can simplify the process of running complex functions in the cloud, making it easier to scale and manage your applications.

## Calling an isolated function inside an isolated function

It is possible to call another isolated function inside an already running isolated function.
This may be useful if you have a function you would like to isolate that depends on another isolated function.

```py
from fal_serverless import isolated

@isolated(requirements=["pyjokes"], machine_type='GPU', keep_alive=10)
def isolated_joke() -> str:
    import pyjokes
    return pyjokes.get_joke()

@isolated(requirements=["cowsay", "fal_serverless"])
def tell_jokes():
    import cowsay
    cowsay.cow(isolated_joke())
    cowsay.fox(isolated_joke())
```

The example above uses the isolated function `isolated_joke` inside of the isolated function `tell_jokes`.
Both of the calls to `isolated_joke` will be handled by fal-serverless separately and a machine will be provisioned as necessary to run them.
Since the environment and the machine type can be different, this is a completely new machine being provisioned.

The `fal-serverless` package needs to be added to the requirements of the second function explicitly.
Notice that authentication is done automatically by the system.
