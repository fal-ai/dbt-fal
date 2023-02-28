<!-- <base href="https://github.com/fal-ai/fal/blob/-/projects/fal/" target="_blank" /> -->

# fal: do more with dbt

fal is the easiest way to run Python with your [dbt](https://www.getdbt.com/) project.

# Introduction

With the `fal` CLI, you can:

- [Send Slack notifications](https://github.com/fal-ai/fal/blob/-/examples/slack-example) upon dbt model success or failure.
- [Load data from external data sources](https://blog.fal.ai/populate-dbt-models-with-csv-data/) before a model starts running.
- [Download dbt models](https://docs.fal.ai/fal/python-package) into a Python context with a familiar syntax: `ref('my_dbt_model')` using `FalDbt`
- [Programatically access rich metadata](https://docs.fal.ai/fal/reference/variables-and-functions) about your dbt project.

Head to our [documentation site](https://docs.fal.ai/) for a deeper dive or play with [in-depth examples](https://github.com/fal-ai/fal/blob/-/examples/README.md) to see how fal can help you get more done with dbt.

> ❗️ If you would like to write data back to your data-warehouse, we recommend using the [`dbt-fal`](https://pypi.org/project/dbt-fal/) adapter.

# Getting Started

## 1. Install `fal`

```bash
$ pip install fal
```

## 2. Go to your dbt project directory

```bash
$ cd ~/src/my_dbt_project
```

## 3. Create a Python script: `send_slack_message.py`

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

## 5. Run `fal flow run`

```bash
$ fal flow run
# both your dbt models and fal scripts are run
```

## 6. Alternatively run `dbt` and `fal` consecutively

```bash
$ dbt run
# Your dbt models are run

$ fal run
# Your python scripts are run
```

# Examples

To explore what is possible with fal, take a look at the in-depth examples below. We will be adding more examples here over time:

- [Example 1: Send Slack notifications](https://github.com/fal-ai/fal/blob/-/examples/slack-example/README.md)
- [Example 2: Use dbt from a Jupyter Notebook](https://github.com/fal-ai/fal/blob/-/examples/write_jupyter_notebook/README.md)
- [Example 3: Read and parse dbt metadata](https://github.com/fal-ai/fal/blob/-/examples/read_dbt_metadata/README.md)
- [Example 4: Metric forecasting](https://github.com/fal-ai/fal/blob/-/examples/metric-forecast/README.md)
- [Example 5: Sentiment analysis on support tickets](https://github.com/fal-ai/fal/blob/-/examples/sentiment-analysis/README.md)
- [Example 6: Anomaly Detection](https://github.com/fal-ai/fal/blob/-/examples/anomaly-detection/README.md)
- [Example 7: Incorporate fal in CI/CD workflow](https://github.com/fal-ai/fal/blob/-/examples/ci_example/README.md)
- [Example 8: Send events to Datadog](https://github.com/fal-ai/fal/blob/-/examples/datadog_event/README.md)
- [Example 9: Write dbt artifacts to GCS](https://github.com/fal-ai/fal/blob/-/examples/write_to_gcs/README.md)
- [Example 10: Write dbt artifacts to AWS S3](https://github.com/fal-ai/fal/blob/-/examples/write_to_aws/README.md)

[Check out the examples directory for more](https://github.com/fal-ai/fal/blob/-/examples/README.md)

# How it works?

`fal` is a command line tool that can read the state of your `dbt` project and help you run Python scripts after your `dbt run`s by leveraging the [`meta` config](https://docs.getdbt.com/reference/resource-configs/meta).

```yaml
models:
  - name: historical_ozone_levels
    ...
    meta:
      fal:
        post-hook:
          # scripts will run concurrently
          - send_slack_message.py
          - another_python_script.py
```

`fal` also provides useful helpers within the Python context to seamlessly interact with dbt models: `ref("my_dbt_model_name")` will pull a dbt model into your Python script as a [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html).

## Running scripts before dbt runs

Run scripts before the model runs by using the `pre-hook:` configuration option.

Given the following schema.yml:

```
models:
  - name: boston
    description: Ozone levels
    config:
      materialized: table
    meta:
      owner: "@meder"
      fal:
        pre-hook:
          - fal_scripts/trigger_fivetran.py
        post-hook:
          - fal_scripts/slack.py
```

`fal flow run` will run `fal_scripts/trigger_fivetran.py`, then the `boston` dbt model, and finally `fal_scripts/slack.py`.
If a model is selected with a selection flag (e.g. `--select boston`), the hooks associated to the model will always run with it.

```bash
$ fal flow run --select boston
```

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
        post-hook:
          - send_slack_message.py
          - another_python_script.py
```

Use the `fal` and `post-hook` keys underneath the `meta` config to let `fal` CLI know where to look for the Python scripts. You can pass a list of scripts as shown above to run one or more scripts as a post-hook operation after a `dbt run`.

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

`context` object also has access to test information related to the current model. If the previous dbt command was either `test` or `build`, the `context.current_model.test` property is populated with a list of tests:

```python
context.current_model.tests
#= [CurrentTest(name='not_null', modelname='historical_ozone_levels, column='ds', status='Pass')]
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

### `write_to_model` function

> ❗️ We recommend using the [`dbt-fal`](https://pypi.org/project/dbt-fal/) adapter for writing data back to your data-warehouse.

It is also possible to send data back to your data-warehouse. This makes it easy to get the data, process it and upload it back into dbt territory.

This function is available in fal Python models only, that is a Python script inside a `fal_models` directory and add a `fal-models-paths` to your `dbt_project.yml`

```yaml
name: "jaffle_shop"
# ...
model-paths: ["models"]
# ...

vars:
  # Add this to your dbt_project.yml
  fal-models-paths: ["fal_models"]
```

Once added, it will automatically be run by fal without having to add any extra configurations in the `schema.yml`.

```python
source_df = source('source_name', 'table_name')
ref_df = ref('a_model')

# Your code here
df = ...

# Upload a `pandas.DataFrame` back to the datawarehouse
write_to_model(df)
```

`write_to_model` also accepts an optional `dtype` argument, which lets you specify datatypes of columns. It works the same way as `dtype` argument for [`DataFrame.to_sql` function](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html).

```python
from sqlalchemy.types import Integer
# Upload but specifically create the `value` column with type `integer`
# Can be useful if data has `None` values
write_to_model(df, dtype={'value': Integer()})
```

## Importing `fal` as a Python package

You may be interested in accessing dbt models and sources easily from a Jupyter Notebook or another Python script.
For that, just import the `fal` package and intantiate a FalDbt project:

```py
from fal import FalDbt
faldbt = FalDbt(profiles_dir="~/.dbt", project_dir="../my_project")

faldbt.list_sources()
# [
#    DbtSource(name='results' ...),
#    DbtSource(name='ticket_data_sentiment_analysis' ...)
#    ...
# ]

faldbt.list_models()
# [
#    DbtModel(name='zendesk_ticket_data' ...),
#    DbtModel(name='agent_wait_time' ...)
#    ...
# ]


sentiments = faldbt.source('results', 'ticket_data_sentiment_analysis')
# pandas.DataFrame
tickets = faldbt.ref('stg_zendesk_ticket_data')
# pandas.DataFrame
```

# Supported `dbt` versions

The latest `fal` version currently supports dbt `1.4.*`.

If you need another version, [open an issue](https://github.com/fal-ai/fal/issues/new) and we will take a look!

# Contributing / Development

We use Poetry for dependency management and easy development testing.

Use Poetry shell to trying your changes right away:

```sh
~ $ cd fal

~/fal $ poetry install

~/fal $ poetry shell
Spawning shell within [...]/fal-eFX98vrn-py3.8

~/fal fal-eFX98vrn-py3.8 $ cd ../dbt_project

~/dbt_project fal-eFX98vrn-py3.8 $ fal flow run
19:27:30  Found 5 models, 0 tests, 0 snapshots, 0 analyses, 165 macros, 0 operations, 0 seed files, 1 source, 0 exposures, 0 metrics
19:27:30 | Starting fal run for following models and scripts:
[...]
```

## Running tests

Tests rely on a Postgres database to be present, this can be achieved with docker-compose:

```sh
~/fal $ docker-compose -f tests/docker-compose.yml up -d
Creating network "tests_default" with the default driver
Creating fal_db ... done

# Necessary for the import test
~/fal $ dbt run --profiles-dir tests/mock/mockProfile --project-dir tests/mock
Running with dbt=1.0.1
[...]
Completed successfully
Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5

~/fal $ pytest -s
```

# Why are we building this?

We think `dbt` is great because it empowers data people to get more done with the tools that they are already familiar with.

`dbt`'s SQL only design is powerful, but if you ever want to get out of SQL-land and connect to external services or get into Python-land for any reason, you will have a hard time. We built `fal` to enable Python workloads (sending alerts to Slack, building predictive models, pushing data to non-data-warehouse destinations and more) **right within `dbt`**.

This library will form the basis of our attempt to more comprehensively enable **data science workloads** downstream of `dbt`. And because having reliable data pipelines is the most important ingredient in building predictive analytics, we are building a library that integrates well with dbt.

# Have feedback or need help?

- Join us in [fal on Discord](https://discord.com/invite/Fyc9PwrccF)
- Join the [dbt Community](http://community.getdbt.com/) and go into our [#tools-fal channel](https://getdbt.slack.com/archives/C02V8QW3Q4Q)
