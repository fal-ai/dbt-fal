---
sidebar_position: 3
---

# Run Python before and after dbt models

Python scripts can be attached as either to run before or after a model. You have to add them in the `meta` section of the dbt model config to use:

```yaml
models:
  - name: modela
    meta:
      fal:
        pre-hook:
          - prepare.py
          - other.py
        post-hook:
          - send_slack_message.py
```

### `after` and `before` scripts vs `post-hook`s and `pre-hook`s

Starting with `0.4.0`, we are deprecating `after` (and `before`) scripts in favor of the newly
introduced `post-hook`s (and `pre-hook`s) for `fal flow run`. The table below is a general
overview between these two features:

|                                                                 | `after` scripts  | `post-hook`s                                                  |
| --------------------------------------------------------------- | ---------------- | ------------------------------------------------------------- |
| Runs after the bound model                                      | ✅               | ✅                                                            |
| Run before dependant models (Runs _as part_ of the bound model) | ✅ (after 0.4.0) | ✅                                                            |
| Parallelization (thread-level) is enabled                       | ✅               | ✅                                                            |
| Runs even if the underlying model's `dbt run` fails             | ❌               | ✅                                                            |
| Can be [parameterized](./structured-hooks.md)                   | ❌               | ✅                                                            |
| Can be individually selected / executed                         | ✅               | ❌                                                            |
| Can use `write_to_source`/`write_to_model`                      | ✅               | ❌ (use [Python models](/fal/python-models/overview) instead) |

If you have an existing project and want to move away from `after` scripts, please see the related section in the ["Compatibility" page](../compatibility.md).
