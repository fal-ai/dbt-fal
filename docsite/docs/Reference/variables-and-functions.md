---
sidebar_position: 1
---

# Variables and functions

Inside a Python script, you get access to some useful variables and functions

## `context` Variable

A `context` object with information relevant to the model through which the script was run. For the [`meta` Syntax](#meta-syntax) example, we would get the following:

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

`context` object also has access to test information related to the current model. If the previous dbt command was either `test` or `build`, the `context.current_model.test` property is populated with a list of tests:

```python
context.current_model.tests
#= [CurrentTest(name='not_null', modelname='historical_ozone_levels, column='ds', status='Pass')]
```

## `ref` and `source` functions

There are also available some familiar functions from `dbt`

```python
# Refer to dbt models or sources by name and returns it as `pandas.DataFrame`
ref('model_name')
source('source_name', 'table_name')

# You can use it to get the running model data
ref(context.current_model.name)
```

## `write_to_source` function

It is also possible to send data back to your datawarehouse. This makes it easy to get the data, process it and upload it back into dbt territory.

All you have to do is define the target source in your schema and use it in fal.
This operation appends to the existing source by default and should only be used targetting tables, not views.

```python
# Upload a `pandas.DataFrame` back to the datawarehouse
write_to_source(df, 'source_name', 'table_name2')
```

`write_to_source` also accepts an optional `dtype` argument, which lets you specify datatypes of columns. It works the same way as `dtype` argument for [`DataFrame.to_sql` function](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html).

```python
from sqlalchemy.types import Integer
# Upload but specifically create the `value` column with type `integer`
# Can be useful if data has `None` values
write_to_source(df, 'source', 'table', dtype={'value': Integer()})
```

### `airbyte_sync` and `fivetran_sync` functions

These functions let you run sync jobs on respective [Airbyte](https://docs.airbyte.com/) and [Fivetran](https://fivetran.com/docs/getting-started) connections and connectors. API connection details as well information on connectors have to be provided in [`profiles.yml`](../Docs/credentials-profile#exctract-load-configuration).

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
airbyte_sync(config_name="my_airbyte_el", connection_name="airbyte_connection_1")
```

`airbyte_sync` triggers and waits for a sync job on an Airbyte connections. Inputs:
- `config_name` - name of the API configuration defined in `profiles.yml`
- `connection_name` - target Airbyte connection name (Optional if `connection_id` is provided)
- `connection_id` - target Airbyte connection id (Optional if `connection_name` is provided)
- `poll_interval` - wait time between polling of a running job (Optional, default is 10 seconds)
- `poll_timeout` - timeout on polling requests (Optional, default is none)

Running a Fivetran sync job is similar:

```python
fivetran_sync(config_name="my_fivetran_el", connector_name="fivetran_connector_1")
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

Use the `fal` and `scripts` keys underneath the `meta` config to let fal CLI know where to look for the Python scripts. You can pass a list of scripts as shown above to run one or more scripts as a post-hook operation after a dbt run.
