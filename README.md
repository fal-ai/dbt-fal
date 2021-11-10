# fal: Do more with dbt
fal allows you to run python scripts directly from your [dbt](https://www.getdbt.com/) project.

With fal, you can:
- Send Slack notifications upon model success or failure.
- Download dbt models into a Python context with a familiar syntax: `ref('my_dbt_model')`
- Use python libraries such as `sklearn` or `prophet` to build more complex pipelines downstream of dbt models.

and more...

Check out [Getting Started](#getting-started) to get a quick start or play with [in-depth examples](#next-steps) to see how `fal` can help you get more done.


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
## 4. Add `fal` `meta` to your `schema.yml`
```yaml
models:
  - name: boston
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
          - slack.py
```
## 5. Run `dbt` and `fal` after each other
```bash
$ dbt run
# Your dbt models are ran

$ fal run
# Your python scripts are ran
```


# Why are we building this?
We think `dbt` is great because it empowers data people to get more done with the tools that they are already familiar with. 

From the perspective of an analytics engineer or a data analyst, `dbt`'s SQL only design is very powerful. But if you ever want to get out of SQL-land and get into Python-land for any reason, you will have a hard time. We built `fal` to enable Python workloads (sending alerts to Slack, building predictive models, pushing data to non-data warehose destinations and more) **right within `dbt`**.

# Concepts
## `meta` Syntax
Here we explain how you can use `meta`

For a complete reference of how `fal` leverages the `meta` property, go [here](docs/meta-reference.md).

## Variables
Here we explain the variables you have access to within the Python context

## Lifecycle Management
Here we talk about how we handle state / `--all` etc.

# Next steps
- [Example 1: Send Slack notifications](docs/slack-example.md)
- [Example 2: Metric forecasting](docs/metric-forecast.md)
- [Example 3: Anomaly detection](docs/anomaly-detection.md)
- [Example 4: Sentiment analysis on support tickets](docs/sentiment-analysis.md)
- [`meta` reference](docs/meta-reference.md)

# Need help?
[Join us in #fal on Discord](https://discord.gg/)
