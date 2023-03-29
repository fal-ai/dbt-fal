---
sidebar_position: 2
---

# Horizontal - Managing Concurrency

Isolated functions in Python, decorated with the `@isolated` decorator, have a convenient `submit` method that allows you to run the function as an asynchronous task. When you call the `submit` method, the isolated function returns an instance of the [`Future` class](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future) from the [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html#module-concurrent.futures) module.

A `Future` represents the result of an asynchronous operation. You can use the `Future` object to wait for the operation to complete and retrieve the result. This makes it easy to run isolated functions in the background, freeing up the main thread to perform other tasks.

Here's an example that demonstrates how to use the `submit` method to run an isolated function as an asynchronous task:

```python
from fal_serverless import isolated
from concurrent.futures import as_completed

@isolated()
def my_function(x, y):
    return x + y

# Call the submit method to run the function as an asynchronous task
future = my_function.submit(2, 3)

# Wait for the task to complete and retrieve the result
result = future.result()

print(result) # Output: 5
```

In this example, we define an isolated function `my_function` that takes two arguments x and y and returns their sum. We then call the `submit` method to run the function as an asynchronous task, passing in the arguments 2 and 3. The `submit` method returns a `Future` object that represents the result of the asynchronous operation. Finally, we use the `result` method on the `Future` object to wait for the task to complete and retrieve the result, which is 5.

One of the benefits of using the `submit` method to run isolated functions as asynchronous tasks is that it allows you to take advantage of concurrency. By running multiple instances of an isolated function in parallel, you can perform tasks more quickly and efficiently.

Here's an example that demonstrates how to use the `submit` method to take advantage of concurrency:

```python
import time
from fal_serverless import isolated
from concurrent.futures import as_completed

@isolated()
def my_function(x):
    time.sleep(x)
    return x

# Call the submit method many times to run multiple instances of the function
futures = [my_function.submit(i) for i in range(10)]

# Wait for all the tasks to complete
start_time = time.time()
for future in as_completed(futures):
    result = future.result()
    print(f"Task {result} completed in {time.time() - start_time:.2f} seconds")
```

In this example, we define an isolated function `my_function` that takes one argument x and simulates a long-running task by sleeping for x seconds. We then use a list comprehension to call the `run` method many times, passing in different arguments each time. We use the `as_completed` function to wait for all the tasks to complete and retrieve the results.

When you run this code, you'll see that the tasks complete in a shorter amount of time compared to if you ran the tasks sequentially. This is because the tasks are run in parallel, taking advantage of the isolated environments.

