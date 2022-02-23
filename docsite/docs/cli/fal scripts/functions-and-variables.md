# Variables and functions

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

### `write_to_source` function

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
