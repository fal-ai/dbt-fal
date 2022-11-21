---
sidebar_position: 1
---

# Variables and functions

Inside a Python script, you get access to some useful variables and functions.

## `context` Variable

`context` is an object with information about the current script context.

### `context.current_model`

This propery holds information relevant to the model, which is associated with the running script. For the [`meta` Syntax](#meta-syntax) example, we would get the following:

```python
context.current_model.name
# str
#= historical_ozone_levels

context.current_model.status
# NodeStatus, enum: 'success' | 'error' | 'skipped'

context.current_model.columns
# Dict[str, ColumnInfo(name: str, tags: List[str], meta: Dict)]

context.current_model.tests
# List[CurrentTest(name: str, modelname: str, column: str, status: str)]

context.current_model.meta
# meta information in the schema.yml
#= {'owner': '@me'}
```

`context.current_model` object also has access to test information related to the current model. If the previous dbt command was either `test` or `build`, the `context.current_model.test` property is populated with a list of tests:

```python
context.current_model.tests
#= [CurrentTest(name='not_null', modelname='historical_ozone_levels, column='ds', status='Pass')]
```

Another relevant property of the `current_model` is `adapter_response`. It contains information that was received from the dbt SQL adapter after computing the model:

```python
context.current_model.adapter_response
#= CurrentAdapterResponse(message='SELECT 10', code='SELECT', rows_affected=10)
```

## Read functions

The familiar dbt functions `ref` and `source` are available in fal scripts to read the models and sources as a Pandas DataFrame.

### `ref` function

The `ref` function is used exactly like in `dbt`. You reference a model in your project

```py
# returned as `pandas.DataFrame`
df = ref('model_name')
```

Or a package model (package first, model second)

```py
df = ref('dbt_artifacts', 'dim_dbt__exposures')
```

You can use the context variable to the the associated model data

```py
df = ref(context.current_model.name)
```

### `source` function

The `source` function is used exactly like in `dbt`. You reference a source in your project

```py
# returned as `pandas.DataFrame`
df = source('source_name', 'table_name')
```

### `execute_sql` function

You can execute artbitrary SQL from within your Python scripts and get results as pandas DataFrames:

```python
my_df = execute_sql('SELECT * FROM {{ ref("my_model") }}')
```

As you can see, the query strings support jinja.

Note that the use of `ref` inside `execute_sql` does not create a node in a dbt dag. So in the case of Python models, you still need to specify dependencies in a comment at the top of the file. For more details, [see here](../fal-dbt/python-models.md#dependencies-on-other-models).

### `list_models` function

You can access model information for all models in the dbt project:

```python
my_models = list_models()

my_models[0].status
# <NodeStatus.Success: 'success'>

my_models[0].name
# 'zendesk_ticket_data'
```

`list_models` returns a list of `DbtModel` objects that contain model and related test information.

### `list_sources` function

You can access source information for all sources in the dbt project:

```python
my_sources = list_sources()

my_sources[0].name
# 'zendesk_ticket_data'

my_sources[0].tests
# []
```

`list_sources` returns a list of `DbtSource` objects that contain source and related test information.

## Write functions

It is also possible to send data back to your data warehouse. This makes it easy to get the data, process it, and upload it back into dbt territory.

### `write_to_source` function

You first have to define the source in your schema.
This operation appends to the existing source by default and should only be used targetting tables, not views.

```python
# Upload a `pandas.DataFrame` back to the data warehouse
write_to_source(df, 'source_name', 'table_name2')
```

`write_to_source` also accepts an optional `dtype` argument, which lets you specify datatypes of columns. It works the same way as `dtype` argument for [`DataFrame.to_sql` function](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html).

```python
from sqlalchemy.types import Integer
# Upload but specifically create the `value` column with type `integer`
# Can be useful if data has `None` values
write_to_source(df, 'source', 'table', dtype={'value': Integer()})
```

### `write_to_model` function

This operation overwrites the existing relation by default and should only be used targetting tables, not views.

For example, if the script is attached to the `zendesk_ticket_metrics` model,

```yaml
models:
  - name: zendesk_ticket_metrics
    meta:
      fal:
        scripts:
          after:
            - from_zendesk_ticket_data.py
```

`write_to_model` will write to the `zendesk_ticket_metrics` table:

```python
df = faldbt.ref('stg_zendesk_ticket_data')
df = add_zendesk_metrics_info(df)

# Upload a `pandas.DataFrame` back to the data warehouse
write_to_model(df) # writes to attached model: zendesk_ticket_metrics
```

> NOTE: When used with `fal flow run` or `fal run` commands, `write_to_model` does not accept a model name, it only operates on the associated model.

But when importing `fal` as a Python module, you have to specify the model to write to:

```python
from fal import FalDbt
faldbt = FalDbt(profiles_dir="~/.dbt", project_dir="../my_project")

faldbt.list_models()
# [
#    DbtModel(name='zendesk_ticket_data' ...),
#    DbtModel(name='agent_wait_time' ...)
# ]

df = faldbt.ref('stg_zendesk_ticket_data')
df = add_zendesk_metrics_info(df)

faldbt.write_to_model(df, 'zendesk_ticket_metrics') # specify the model
```

### Specifying column types

The functions `write_to_source` and `write_to_model` also accept an optional `dtype` argument, which lets you specify datatypes of columns.
It works the same way as `dtype` argument for [`DataFrame.to_sql` function.](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html)

```python
from sqlalchemy.types import Integer

# Upload but specifically create the `my_col` column with type `integer`
# Can be specially useful if data has `None` values
write_to_source(df, 'source', 'table', dtype={'my_col': Integer()})
```

### Modes of _writing_

These functions accepts two modes of _writing_: `append` and `overwrite`.

They are passed with the optional `mode` argument (`append` is the default value).

```python
# Overwrite the table with the dataframe data, deleting old data
write_to_source(df, 'source_name', 'table_name', mode='overwrite')
write_to_model(df, 'model_name', mode='overwrite') # default mode

# Append more data to the existing table (create it if it does not exist)
write_to_source(df2, 'source_name', 'table_name', mode='append') # default mode
write_to_model(df2, 'model_name', mode='apend')
```

#### The `append` mode

1. creates the table if it does not exist yet
2. insert data into the table

#### The `overwrite` mode

1. creates a temporal table
2. insert data into the temporal table
3. drops the old table if it exists
4. renames the temporal table to the final table name

## Extract-Load pipelines

`fal` provides a module `el` that lets you run EL jobs. `el` has two methods: `el.airbyte_sync` and `el.fivetran_sync`. These methods let you run sync jobs on respective [Airbyte](https://docs.airbyte.com/) and [Fivetran](https://fivetran.com/docs/getting-started) connections and connectors. API connection details as well information on connectors have to be provided in [`profiles.yml`](../credentials-profile.md#extract-load-configuration).

Given this bit of `profiles.yml`:

```yaml
  fal_extract_load:
    dev:
      my_fivetran_el
        type: fivetran
        api_key: my_fivetran_key
        api_secret: my_fivetran_secret
        connectors:
          - name: fivetran_connector_1
            id: id_fivetran_connector_1
      my_airbyte_el
        type: airbyte
        host: http://localhost:8001
        connections:
          - name: airbyte_connection_1
            id: id_airbyte_connection_1
```

you can run an Airbyte sync job:

```python
el.airbyte_sync(config_name="my_airbyte_el", connection_name="airbyte_connection_1")
```

`airbyte_sync` triggers and waits for a sync job on an Airbyte connections. Inputs:

- `config_name` - name of the API configuration defined in `profiles.yml`
- `connection_name` - target Airbyte connection name (Optional if `connection_id` is provided)
- `connection_id` - target Airbyte connection id (Optional if `connection_name` is provided)
- `poll_interval` - wait time between polling of a running job (Optional, default is 10 seconds)
- `poll_timeout` - timeout on polling requests (Optional, default is none)

Running a Fivetran sync job is similar:

```python
el.fivetran_sync(config_name="my_fivetran_el", connector_name="fivetran_connector_1")
```

`fivetran_sync` triggers and waits for a sync job on a Fivetran connector. Inputs:

- `config_name` - name of the API configuration defined in `profiles.yml`
- `connector_name` - target Fivetran connector name (Optional if `connector_id` is provided)
- `connection_id` - target Fivetran connector id (Optional if `connector_name` is provided)
- `poll_interval` - wait time between polling of a running job (Optional, default is 10 seconds)
- `poll_timeout` - timeout on polling requests (Optional, default is none)

## `meta` syntax

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

Use the `fal` and `scripts` keys underneath the `meta` config to let fal CLI know where to look for the Python scripts.
You can pass a list of scripts as shown above to run one or more scripts as a post-hook operation after a dbt run.
