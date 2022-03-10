---
sidebar_position: 1
---

# Referring to dbt models and sources from a Python context

fal introduces variables and functions in the context of a script to make it easier to interact with your data.

You can reference a model just like you do from dbt with a simple use of the familiar `ref` function. The same can be done for source relations with the `source` function.

```py
model_df = ref('my_model') # pandas.DataFrame
source_df = source('schema', 'table') # pandas.DataFrame
```

And when a script is attached to a model, the context also includes information about the model the script is attached to:

```py
context.current_model.name
context.current_model.meta.get("owner")
```

Review the [reference](../../Reference/variables-and-functions.md) for a more thorough view of this subject.
