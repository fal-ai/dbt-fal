<!-- <base href="https://github.com/fal-ai/fal/blob/-/projects/adapter/" target="_blank" /> -->

# Welcome to dbt-fal 👋 do more with dbt

dbt-fal adapter is the ✨easiest✨ way to run your [dbt Python models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/python-models).

Starting with dbt v1.3, you can now build your dbt models in Python. This leads to some cool use cases that was once difficult to build with SQL alone. Some examples are:

- Using Python stats libraries to calculate stats
- Building forecasts
- Building other predictive models such as classification and clustering

This is fantastic! BUT, there is still one issue though! The developer experience with Snowflake and Bigquery is not great, and there is no Python support for Redshift and Postgres.

dbt-fal provides the best environment to run your Python models that works with all other data warehouses! With dbt-fal, you can:

- Build and test your models locally
- Isolate each model to run in its own environment with its own dependencies
- [Coming Soon] Run your Python models in the ☁️ cloud ☁️ with elasticly scaling Python environments.
- [Coming Soon] Even add GPUs to your models for some heavier workloads such as training ML models.

## Getting Started

### 1. Install dbt-fal
`pip install dbt-fal[bigquery,snowflake]` *Add your current warehouse here*

### 2. Update your `profiles.yml` and add the fal adapter

```yaml
jaffle_shop:
  target: dev_with_fal
  outputs:
    dev_with_fal:
      type: fal
      db_profile: dev_bigquery # This points to your main adapter
    dev_bigquery:
      type: bigquery
      ...
```

Don't forget to point to your main adapter with the `db_profile` attribute. This is how the fal adapter knows how to connect to your data warehouse.

### 3. `dbt run`!
That is it! It is really that simple 😊

### 4. [🚨 Cool Feature Alert 🚨] Environment management with dbt-fal
If you want some help with environment management (vs sticking to the default env that the dbt process runs in), you can create a fal_project.yml in the same folder as your dbt project and have “named environments”:

In your dbt project folder:
```bash
$ touch fal_project.yml

# Paste the config below
environments:
  - name: ml
    type: venv
    requirements:
      - prophet
```

and then in your dbt model:

```bash
$ vi models/orders_forecast.py

def model(dbt, fal):
    dbt.config(fal_environment="ml") # Add this line

    df: pd.DataFrame = dbt.ref("orders_daily")
```

The `dbt.config(fal_environment=“ml”)` will give you an isolated clean env to run things in, so you dont pollute your package space.

### 5. [Coming Soon™️] Move your compute to the Cloud!
Let us know if you are interested in this. We are looking for beta users.

# `dbt-fal` command line tool

With the `dbt-fal` CLI, you can:

- [Send Slack notifications](https://github.com/fal-ai/fal/blob/-/examples/slack-example) upon dbt model success or failure.
- [Load data from external data sources](https://blog.fal.ai/populate-dbt-models-with-csv-data/) before a model starts running.
- [Download dbt models](https://docs.fal.ai/fal/python-package) into a Python context with a familiar syntax: `ref('my_dbt_model')` using `FalDbt`
- [Programatically access rich metadata](https://docs.fal.ai/fal/reference/variables-and-functions) about your dbt project.

Head to our [documentation site](https://docs.fal.ai/) for a deeper dive or play with [in-depth examples](https://github.com/fal-ai/fal/blob/-/examples/README.md) to see how fal can help you get more done with dbt.


## 1. Install `dbt-fal`

```bash
$ pip install dbt-fal[postgres]
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

## 5. Run `dbt-fal flow run`

```bash
$ dbt-fal flow run
# both your dbt models and python scripts are run
```

## 6. Alternatively run `dbt` and `fal` consecutively

```bash
$ dbt run
# Your dbt models are run

$ dbt-fal run
# Your python scripts are run
```


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

`dbt-fal flow run` will run `fal_scripts/trigger_fivetran.py`, then the `boston` dbt model, and finally `fal_scripts/slack.py`.
If a model is selected with a selection flag (e.g. `--select boston`), the hooks associated to the model will always run with it.

```bash
$ dbt-fal flow run --select boston
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
from fal.dbt import FalDbt
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

# Why are we building this?

We think `dbt` is great because it empowers data people to get more done with the tools that they are already familiar with.

This library will form the basis of our attempt to more comprehensively enable **data science workloads** downstream of `dbt`. And because having reliable data pipelines is the most important ingredient in building predictive analytics, we are building a library that integrates well with dbt.

# Have feedback or need help?

- Join us in [fal on Discord](https://discord.com/invite/Fyc9PwrccF)
- Join the [dbt Community](http://community.getdbt.com/) and go into our [#tools-fal channel](https://getdbt.slack.com/archives/C02V8QW3Q4Q)
