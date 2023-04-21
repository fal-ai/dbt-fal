---
sidebar_position: 5
---

# Storage and Persistence

The target environments of isolated functions have access to a directory `/data`. The `/data` directory is persistent and user-specific. This directory allows you to store and retrieve data across function invocations, enabling you to persist data between executions.

Accessing the `/data` directory is simple, as it is automatically mounted in the function's environment. You can read from and write to the `/data` directory just as you would with any other directory in your file system.

Here are two examples to demonstrate the use of the `/data` directory:

**Example 1: Storing User Preferences**

Let's say you have a function that generates personalized recommendations for users based on their preferences. The function can store the preferences in the `/data` directory so that they persist across invocations. The code would look something like this:

```python
@isolated()
def generate_recommendations(user_id, preferences):
    preferences_file = f"/data/user_{user_id}_preferences.txt"
    with open(preferences_file, "w") as f:
        f.write(preferences)
    # Generate recommendations based on the stored preferences
    # ...
```

**Example 2: Persisting Model Weights**

In machine learning, you may want to persist the model weights so that you don't have to download them every time you need to make a prediction. The `/data` directory provides an easy way to persist the model weights between function invocations.

Here's an example of how you might use the `/data` directory to store model weights in a deep learning scenario:

```python
import os
import tensorflow as tf
from fal_serverless import isolated

@isolated(requirements=["tensorflow"])
def train_and_predict(data, model_weights_file='/data/model_weights.h5'):
    model = create_model()
    if os.path.exists(model_weights_file):
        model.load_weights(model_weights_file)
    else:
        model.fit(data)
        model.save_weights(model_weights_file)
    return model.predict(data)

def create_model():
    model = tf.keras.Sequential([
        tf.keras.layers.Dense(64, activation='relu'),
        tf.keras.layers.Dense(64, activation='relu'),
        tf.keras.layers.Dense(10, activation='softmax')
    ])
    model.compile(optimizer='adam',
                  loss='categorical_crossentropy',
                  metrics=['accuracy'])
    return model
```

In this example, the function `train_and_predict` first checks if the model weights file `model_weights.h5` exists in the `/data` directory. If it does, the function loads the weights into the model. If not, the function trains the model and saves the weights to the `/data` directory. This way, on subsequent invocations, the function can simply load the weights from the `/data` directory, which will be much faster than retraining the model from scratch.

## `sync_dir` Function

The `sync_dir` function allows you to easily upload local directories to the persistent `/data` directory. Here's an example of how to use the sync_dir function:

```python
from fal_serverless import sync_dir, isolated

# Upload a local directory to the persistent /data directory
sync_dir("path/to/local/dir", "remote_dir")

# An isolated function to list the contents of the uploaded directory
@isolated()
def test():
    import os
    os.system("ls /data/sync/remote_dir")

# Execute the test function
test()  # prints contents of the uploaded directory
```

In this example, the local directory specified by `path/to/local/dir` is uploaded to `/data/sync/remote_dir` in the fal-serverless environment.
