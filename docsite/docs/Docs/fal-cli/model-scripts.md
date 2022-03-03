---
sidebar_position: 2
---

# Running Python scripts as pre and post hooks

Python scripts can be attached as pre-hook (`before`) or post-hook (`after`) to a model. You have to add them in the `meta` section of the dbt model to use:

```yaml
models:
- name: modela
  meta:
    fal:
      scripts:
        before:
        - prepare.py
        - other.py
        after:
        - send_slack_message.py
```

Then when running `fal flow run`, the scripts are run in order of the list, first the _before_, then dbt models, and finally the _after_ scripts.
