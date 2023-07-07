---
sidebar_position: 1
---

# Quickstart

fal is the easiest way to run Python with your dbt project.

The fal ecosystem has two main components: the dbt-fal adapter and the fal CLI.

### With the dbt-fal Python adapter, you can:

- Enable a developer-friendly Python environment for most databases, including ones without dbt Python support such as Redshift, Postgres.
- Use Python libraries such as sklearn or prophet to build more complex dbt models including ML models.
- Easily manage your Python environments with isolate.
- Iterate on your Python models locally and then scale them out in the cloud.

Go to the [dbt-fal](/dbt-fal/python-models/overview) documentation for more details!

### With the fal CLI, you can:

- Send Slack notifications upon dbt model success or failure.
- Load data from external data sources before a model starts running.
- Download dbt models into a Python context with a familiar syntax: `ref('my_dbt_model')`
- Programatically access rich metadata about your dbt project using `FalDbt`.

Go to the [fal CLI](/dbt-fal/orchestrate-dbt-runs/) documentation for more details!

## 1. Install fal and dbt-fal

```bash
$ pip install fal dbt-fal[postgres]
```

## 2. Go to your dbt directory

```bash
$ cd ~/src/my_dbt_project
```

## 3. Create a Python script: `send_slack_message.py`

```python
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

CHANNEL_ID = os.getenv("SLACK_CHANNEL")
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")

client = WebClient(token=SLACK_TOKEN)

message_text = f"Model: {context.current_model.name}. Status: {context.current_model.status}."

if str(context.current_model.status) == 'success':
  # Read model as pandas.DataFrame
  df = ref(context.current_model.name)
  message_text += f" Size: {df.size}."

try:
    response = client.chat_postMessage(
        channel=CHANNEL_ID,
        text=message_text
    )
except SlackApiError as e:
    assert e.response["error"]
```

As you can see from the `context` object, fal makes certain variables (and functions) avaliable in this script. [Check out the fal scripts section for more details](./reference/variables-and-functions.md)

## 4. Add a `meta` section in your `schema.yml`

```yaml
models:
  - name: some_model
    meta:
      fal:
        scripts:
          - send_slack_message.py
```

## 5. Run `fal flow run`

This command manages your dbt runs for you, by running scripts and models in the correct order.

```bash
$ fal flow run
# 1. dbt model `some_model` is run
# 2. slack message is sent with the run result
```
