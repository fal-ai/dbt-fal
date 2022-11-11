---
sidebar_position: 2
---

# What is fal?

fal is a command line tool that can read the state of your dbt project and help you run Python scripts with dbt by leveraging the [`meta` config](https://docs.getdbt.com/reference/resource-configs/meta).

```yaml
models:
  - name: historical_ozone_levels
    ...
    meta:
      fal:
        scripts:
          - send_slack_message.py
          - another_python_script.py # will be run after the first script
```

fal also provides useful helpers within the Python context to seamlessly interact with dbt models: `ref("my_dbt_model_name")` will pull a dbt model into your Python script as a [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html).
