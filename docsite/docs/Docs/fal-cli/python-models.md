---
sidebar_position: 2
---

# Making Python dbt models

> ⚠️ NOTE: This feature is enabled with the flag `--experimental-models`

There are some cases where a SQL transformation is either too complex or impossible. With `fal flow` you can build your dbt models in Python.

To achieve this, add the Python (`.py`) or [Notebook (`.ipynb`)](./notebook-files.md) files to any of your [`model-paths`](https://docs.getdbt.com/reference/project-configs/model-paths) and make sure your script calls the [`write_to_model`](../../Reference/variables-and-functions.md#writetomodel-function) function _exactly once_.

## Dependencies

`fal` should pick up most usages of [`source`](../../Reference/variables-and-functions.md#source-function) or [`ref`](../../Reference/variables-and-functions.md#ref-function) functions and generate the appropriate dependency for the `dbt` DAG. In case a dependency is not being picked up by `fal`, you can specify them in the top of the Python file in the [module docstring](https://peps.python.org/pep-0257/):
```py
"""Generates Python model with forecast data

For fal to pick up these dependencies:
- ref('model_a')
- source('database_b', 'table_b')
"""

from prophey import Prophet

# fal will pick up this dependency
df_c = ref('model_c')

# calculate dataframe
df = some_calc(df_c)

write_to_model(df)
```

## Generated files

When running `fal flow run --experimental-models`, it will automatically generate a sql file for each Python file found. This is so `dbt` _knows_ about the Python model, which enables using the Python model downstream back in `dbt`. 

These generated files should not be modified directly by the user. They will look like this:
```sql
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED f3d686c040e94a5b33aa082f0ddcd6d3

Script dependencies:

{{ ref('model_a') }}
{{ source('database_b', 'table_b') }}
{{ ref('model_c') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.name }}
```
And the `FAL_GENERATED <checksum>` line is to check we are not overwritting any user files.
