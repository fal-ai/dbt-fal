---
sidebar_position: 2
---

# Running Python before and after dbt models

Python scripts can be attached as pre-hook (`before`) or post-hook (`after`) to a model. You have to add them in the `meta` section of the dbt model config to use:

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

Then when running `fal flow run`, first before scripts are ran, followed by dbt models and finally the after scripts.
