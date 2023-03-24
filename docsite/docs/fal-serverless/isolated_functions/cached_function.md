---
sidebar_position: 4
---

# Cached functions: Caching the Output of Functions for Improved Performance
The `@cached` decorator is a tool for improving the performance of your isolated function. When a function is decorated with `@cached`, it is referred to as a "cached function."

A cached function can be called inside an isolated function, and the output of the cached function is cached. If the cached function is called in an isolated environment and the environment has been [kept alive](./keep_alive) since the last time the same cached function was called, then the cached function is not executed. Instead, a cached return value is returned. This can significantly improve the performance of your functions by reducing the time it takes to repeatedly execute code and minimizing the resource consumption.

Here's an example that demonstrates how to use the `@cached` decorator:

```python
from fal_serverless import isolated, cached

@cached
def my_cached_function(x):
    # Perform some time-consuming calculation
    import time
    time.sleep(10)
    return x ** 2

@isolated(keep_alive=10)
def my_isolated_function(x):
    result = my_cached_function(x)
    return result

# Call the isolated function multiple times
result1 = my_isolated_function(2) # Takes more than 10 seconds
result2 = my_isolated_function(2) # Almost instant

print(result1) # Output: 4
print(result2) # Output: 4
```

In the above example, we have a cached function `my_cached_function` that takes one argument x and performs a time-consuming calculation to return x ** 2. We then have an isolated function `my_isolated_function` that calls the cached function. `my_isolated_function` has `keep_alive` set for 10 seconds. When you try this example, you'll see that the second call does not wait for 10 seconds and returns the same result right away. This is because the `@cached` decorator has cached the output of `my_cached_function` in the isolated environment. Since the environment is kept alive for 10 seconds, the second call to `my_cached_function` returns a cached result, instead of re-executing the function.
