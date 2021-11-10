# fal: do more with dbt
fal allows you to run python scripts directly from your [dbt](https://www.getdbt.com/) project.

With fal, you can:
- Send Slack notifications upon dbt model success or failure.
- Download dbt models into a Python context with a familiar syntax: `ref('my_dbt_model')`
- Use python libraries such as `sklearn` or `prophet` to build more complex pipelines downstream of dbt models.

and more...

Check out [Getting Started](#getting-started) to get a quick start or play with [in-depth examples](#next-steps) to see how fal can help you get more done with dbt.

# Getting Started

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
## 4. Add a `meta` section in your `schema.yml`
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
          - send_slack_message.py
```
## 5. Run `dbt` and `fal` consecutively
```bash
$ dbt run
# Your dbt models are ran

$ fal run
# Your python scripts are ran
```


# Why are we building this?
We think `dbt` is great because it empowers data people to get more done with the tools that they are already familiar with. 

`dbt`'s SQL only design is very powerful, but if you ever want to get out of SQL-land and connect to external services or get into Python-land for any reason, you will have a hard time. We built `fal` to enable Python workloads (sending alerts to Slack, building predictive models, pushing data to non-data warehose destinations and more) **right within `dbt`**.

This library will form the basis of our attempt to more comprehensively enable **data science workloads** downstream of dbt. And because having reliable data pipelines is the most important ingredient in building predictive analytics, we are building a library that integrates really well with dbt.

# How it works?
Here we describe how `fal` works.

# Concepts
## `meta` Syntax
Here we explain how you can use `meta`

For a complete reference of how `fal` leverages the `meta` property, go [here](docs/meta-reference.md).

## Variables
Here we explain the variables you have access to within the Python context

## Lifecycle Management
Here we talk about how we handle state / `--all` etc.

# Examples
To explore what is possible with fal, take a look at the in-depth examples below. We will be adding more examples here over time:
- [Example 1: Send Slack notifications](docs/slack-example.md)
- [Example 2: Metric forecasting](docs/metric-forecast.md)
- [Example 3: Anomaly detection](docs/anomaly-detection.md)
- [Example 4: Sentiment analysis on support tickets](docs/sentiment-analysis.md)

# Have feedback or need help?
[Join us in #fal on Discord](https://discord.gg/)
