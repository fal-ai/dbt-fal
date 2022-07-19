---
sidebar_position: 3
---

# Running Python before and after dbt models

Python scripts can be attached as either to run before or after a model. You have to add them in the `meta` section of the dbt model config to use:

```yaml
models:
  - name: modela
    meta:
      fal:
        scripts:
          before:
            - prepare.py
            - other.py
          post-hook:
            - send_slack_message.py
```

### `after` scripts vs `post-hook`s

Starting with `0.4.0`, we are deprecating `after` scripts in favour of the newly introduced `post-hook`s for `fal flow run`. The table below is a general
overview between these two features:

|                                                                                                            | `after` scripts                      | `post-hook`s                                 |
|------------------------------------------------------------------------------------------------------------|--------------------------------------|----------------------------------------------|
| Runs after the bound model                                                                                 | ✅                                   | ✅                                           |
| Runs **right** after the bound model (guarantee of dependant DBT models will be executed after the script) | ❌ (before 0.4.0) / ✅ (after 0.4.0) | ✅                                           |
| Runs even if the underlying model's `dbt run` fails                                                        | ❌                                   | ✅                                           |
| Parallelization (thread-level) is enabled                                                                  | ✅                                   | ✅                                           |
| Can be individually selected / executed                                                                    | ✅                                   | ❌                                           |
| Can use `write_to_source`/`write_to_model`                                                                 | ✅                                   | ❌ (recommended way is using [Python models](/Docs/fal-cli/python-models))  |

If you have an existing project and want to move away from `after` scripts, please see the related section in the ["Compatibility" page](/Docs/compatibility).
