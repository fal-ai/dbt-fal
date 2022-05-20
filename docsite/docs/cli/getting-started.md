---
sidebar_position: 1
slug: /
---

# Getting Started

Let's discover `fal` **in less than 5 minutes**.

`fal` allows you to run python scripts directly from your `dbt` project.

With `fal`, you can:

- Send Slack notifications upon dbt model success or failure.
- Download dbt models into a Python context with a familiar syntax: `ref('my_dbt_model')`.
- Use python libraries such as sklearn or prophet to build more complex pipelines downstream of dbt models.
  and more...

## 1. Install fal

```bash
$ pip install fal
```

## 2. Go to your dbt directory

```bash
$ cd ~/src/my_dbt_project
```

## 3. Create a python script: `send_slack_message.py`

```python
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

CHANNEL_ID = os.getenv("SLACK_BOT_CHANNEL")
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")

client = WebClient(token=SLACK_TOKEN)
message_text = f"Model: {context.current_model.name}. Status: {context.current_model.status}."

try:
    response = client.chat_postMessage(
        channel=CHANNEL_ID,
        text=message_text
    )
except SlackApiError as e:
    assert e.response["error"]
```

fal makes certain variables and functions avaliable in this script, [checkout the fal scripts section for more details](fal%20scripts/functions-and-variables)

## 4. Add a `meta` section in your `schema.yml`

```yaml
models:
  - name: historical_ozone_levels
    description: Ozone levels
    config:
      materialized: table
    columns:
      - name: ozone_level
        description: Ozone level
      - name: ds
        description: Date
    meta:
      fal:
        scripts:
          - send_slack_message.py
```

## 5. Run `dbt` and `fal` consecutively

```bash
$ dbt run
# Your dbt models are ran

$ fal run
# Your python scripts are ran
```

# How it works?

`fal` is a command line tool that can read the state of your `dbt` project and help you run Python scripts after your `dbt run`s by leveraging the [`meta` config](https://docs.getdbt.com/reference/resource-configs/meta).

```yaml
models:
  - name: historical_ozone_levels
    ...
    meta:
      fal:
        scripts:
          - send_slack_message.py
          - another_python_script.py # will be ran after the first script
```

## Model scripts selection

By default, the `fal run` command runs the Python scripts as a post-hook, **only** on the models that were ran on the last `dbt run`; that means that if you are using model selectors, `fal` will only run on the models `dbt` ran. To achieve this, fal needs the dbt-generated file `run_results.json` available.

If you are running `fal` in a clean environment (no `run_results.json` available) or just want to specify which models you want to run the scripts for, `fal` handles [dbt's selection flags](https://docs.getdbt.com/reference/node-selection/syntax) for `dbt run` as well as offering an extra flag for just running _all_ models:

```
--all                    Run scripts for all models.
-s, --select TEXT        Specify the nodes to include.
-m, --models TEXT        Specify the nodes to include. (Deprecated)
--exclude TEXT           Specify the models to exclude.
--selector TEXT          The selector name to use, as defined in selectors.yml
```

For passing more than one selection or exlusion, you must specify the flag multiple times:

```bash
$ fal run -s model_alpha -s model_beta
... | Starting fal run for following models and scripts:
model_alpha: script.py
model_beta: script.py, other.py
```

`fal` also provides useful helpers within the Python context to seamlessly interact with dbt models: `ref("my_dbt_model_name")` will pull a dbt model into your Python script as a [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html).
