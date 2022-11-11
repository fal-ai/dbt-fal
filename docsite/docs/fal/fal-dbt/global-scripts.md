---
sidebar_position: 5
---

# Running scripts not associated with a dbt model

Typically a [fal script is associated with a dbt model](model-scripts.md), this is how the [context variable is populated](../../Reference/variables-and-functions.md#context-variable). However you may want to invoke scripts independent of a dbt model as well. This can be achieved by adding a script configuration similar to meta for models, but in the schema.yml top level:

```yaml
models:
...

fal:
  scripts:
    before:
      - global/prepare_run.py
    after:
      - global/close_run.py
```

These will happen at the beginning and end of any `fal run`, no matter which models are selected, since these are pre and post hooks of running fal.

The `before` part will run before all model scripts and the `after` will run after all model scripts.
