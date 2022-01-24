# fal: do more with dbt
fal allows you to run python scripts directly from your [dbt](https://www.getdbt.com/) project.

<p align="center">
  <a href="https://pepy.tech/project/fal">
    <img src="https://static.pepy.tech/personalized-badge/fal?period=total&units=international_system&left_color=grey&right_color=blue&left_text=Downloads" alt="Total downloads" />
  </a>&nbsp;
  <a href="https://pypi.org/project/fal/">
    <img src="https://badge.fury.io/py/fal.svg" alt="Angular on npm" />
  </a>&nbsp;
  <a href="https://discord.com/invite/Fyc9PwrccF">
    <img src="https://badgen.net/badge/icon/Join%20Us%20On%20Discord/red?icon=discord&label" alt="Discord conversation" />
  </a>
</p>

With fal, you can:
- Send Slack notifications upon dbt model success or failure.
- Download dbt models into a Python context with a familiar syntax: `ref('my_dbt_model')`
- Use python libraries such as [`sklearn`](https://scikit-learn.org/) or [`prophet`](https://facebook.github.io/prophet/) to build more complex pipelines downstream of `dbt` models.

and more...

Check out our [Getting Started](#getting-started) guide to get a quickstart or play with [in-depth examples](#examples) to see how fal can help you get more done with dbt.

[<img src="https://cdn.loom.com/sessions/thumbnails/bb49fffaa6f74e90b91d26c77f35ecdc-1637262660876-with-play.gif">](https://www.loom.com/share/bb49fffaa6f74e90b91d26c77f35ecdc)


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

# Examples
To explore what is possible with fal, take a look at the in-depth examples below. We will be adding more examples here over time:
- [Example 1: Send Slack notifications](docs/slack-example.md)
- [Example 2: Metric forecasting](docs/metric-forecast.md)
- [Example 3: Sentiment analysis on support tickets](docs/sentiment-analysis.md)
- [Example 4: Send event to Datadog](docs/datadog_event.md)
- [Example 5: Incorporate fal in CI/CD workflow](docs/ci_example.md)
- [Example 6: Send data to Firestore](docs/write_to_firestore.md)
- [Example 7: Write dbt artifacts to GCS](docs/write_to_gcs.md)
- [Example 8: Write dbt artifacts to AWS S3](docs/write_to_aws.md)
- [Example 9: Use dbt from a Jupyter Notebook](docs/write_jupyter_notebook.md)
- [Example 10: Read and parse dbt metadata](docs/read_dbt_metadata.md)
- [Example 11: Anomaly Detection](docs/anomaly-detection.md)

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

By default, the `fal run` command runs the Python scripts as a post-hook, **only** on the models that were ran on the last `dbt run` (So if you are using model selectors, `fal` will only run on the selected models). If you want to run all scripts associated with models, you can do so by using the `--all` flag with the fal CLI.

`fal` also provides useful helpers within the Python context to seamlessly interact with dbt models: `ref("my_dbt_model_name")` will pull a dbt model into your Python script as a [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html).

# Concepts
## profile.yml and Credentials
`fal` integrates with `dbt`'s `profile.yml` file to access and read data from the data warehouse. Once you setup credentials in your `profile.yml` file for your existing `dbt` workflows anytime you use `ref` or `source` to create a dataframe `fal` authenticates using the credentials specified in the `profile.yml` file.

## `meta` Syntax
```yaml
models:
  - name: historical_ozone_levels
    ...
    meta:
      owner: "@me"
      fal:
        scripts:
          - send_slack_message.py
          - another_python_script.py # will be run sequentially
```
Use the `fal` and `scripts` keys underneath the `meta` config to let `fal` CLI know where to look for the Python scripts. You can pass a list of scripts as shown above to run one or more scripts as a post-hook operation after a `dbt run`.

## Variables and functions
Inside a Python script, you get access to some useful variables and functions

### Variables

A `context` object with information relevant to the model through which the script was run. For the [`meta` Syntax](#meta-syntax) example, we would get the following:
```python
context.current_model.name
#= historical_ozone_levels

context.current_model.meta
#= {'owner': '@me'}

context.current_model.meta.get("owner")
#= '@me'

context.current_model.status
# Could be one of
#= 'success'
#= 'error'
#= 'skipped'
```

### `ref` and `source` functions
There are also available some familiar functions from `dbt`
```python
# Refer to dbt models or sources by name and returns it as `pandas.DataFrame`
ref('model_name')
source('source_name', 'table_name')

# You can use it to get the running model data
ref(context.current_model.name)
```

### `write_to_source` function
It is also possible to send data back to your datawarehouse. This makes it easy to get the data, process it and upload it back into dbt territory.

All you have to do is define the target source in your schema and use it in fal. 
This operation appends to the existing source by default and should only be used targetting tables, not views.
```python
# Upload a `pandas.DataFrame` back to the datawarehouse
write_to_source(df, 'source_name', 'table_name2')
```

## Lifecycle and State Management
By default, the `fal run` command runs the Python scripts as a post-hook, **only** on the models that were ran on the last `dbt run` (So if you are using model selectors, `fal` will only run on the selected models).

If you want to run all Python scripts regardless, you can do so by using the `--all` flag with the `fal` CLI:

```bash
$ fal run --all
```

## Importing `fal` as a Python package
You may be interested in accessing dbt models and sources easily from a Jupyter Notebook or another Python script.
For that, just import the `fal` package and intantiate a FalDbt project:

```py
from fal import FalDbt
faldbt = FalDbt(profiles_dir="~/.dbt", project_dir="../my_project")

faldbt.list_sources()
# [['results', 'ticket_data_sentiment_analysis']]

faldbt.list_models()
# {
#   'zendesk_ticket_metrics': <RunStatus.Success: 'success'>,
#   'stg_o3values': <RunStatus.Success: 'success'>,
#   'stg_zendesk_ticket_data': <RunStatus.Success: 'success'>,
#   'stg_counties': <RunStatus.Success: 'success'>
# }

sentiments = faldbt.source('results', 'ticket_data_sentiment_analysis')
# pandas.DataFrame
tickets = faldbt.ref('stg_zendesk_ticket_data')
# pandas.DataFrame
```

# Supported `dbt` versions

Any extra configuration to work with different `dbt` versions is not needed, latest `fal` version currently supports:

- 0.20.*
- 0.21.*
- 1.0.*

If you need another version, [open an issue](https://github.com/fal-ai/fal/issues/new) and we will take a look!

# Why are we building this?
We think `dbt` is great because it empowers data people to get more done with the tools that they are already familiar with.

`dbt`'s SQL only design is powerful, but if you ever want to get out of SQL-land and connect to external services or get into Python-land for any reason, you will have a hard time. We built `fal` to enable Python workloads (sending alerts to Slack, building predictive models, pushing data to non-data warehose destinations and more) **right within `dbt`**.

This library will form the basis of our attempt to more comprehensively enable **data science workloads** downstream of dbt. And because having reliable data pipelines is the most important ingredient in building predictive analytics, we are building a library that integrates well with dbt.


# Have feedback or need help?
[Join us in #fal on Discord](https://discord.com/invite/Fyc9PwrccF)
