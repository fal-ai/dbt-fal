---
sidebar_position: 4
---

# Running scripts not associated to a dbt model

Just like when [running scripts associated to a dbt model](model-scripts.md), you can run a script as pre-hook or post-hook of the whole fal flow operation.

This can be achieved by adding the same script lists as we do in `meta` for models, but in the schema.yml top level:

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

These will happen at the beginning and end of any fal flow run, no matter which models are selected, since these are pre and post hooks of running dbt.

The `before` part will run before all model scripts and the `after` will run after all model scripts.
