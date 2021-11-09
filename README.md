# fal: Do more with dbt
fal allows you to run python scripts directly from your [dbt](https://www.getdbt.com/) project.

With fal, you can:
- Send Slack notifications upon model success or failure.
- Download dbt models into a Python context with a familiar syntax: `ref('my_dbt_model)`
- Use python libraries such as `sklearn` or `prophet` to build more complex pipelines downstream of dbt models.
- and more...


# Getting Started

## 1. Install fal
```bash
$ pip install fal
```

## 2. Go to your dbt directory
```bash
$ cd ~/src/my_dbt_project
```

## 3. Create a python script
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

## 4. Run `dbt` and `fal` after each other
```bash
$ dbt run

$ fal run
```


# Why did we build this?
