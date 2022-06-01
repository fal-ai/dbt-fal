---
sidebar_position: 2
---

# Creating Python dbt models

> ⚠️ NOTE: This feature is enabled with the flag `--experimental-models` when running the `fal flow run` command.

As of version `0.3.0`, fal now supports a new building block: _Pure Python models_. This new type of model allows you to represent a `dbt` model purely in Python code leveraging its rich ecosystem of libraries. With Python models, you can now go beyond the capabilities of SQL and build:

1. Data transformations that Python is better suited for e.g. text manipulation, leveraging external libraries.
2. Use Data Science libraries such as sklearn, xgboost to represent ML model artifacts such as predictions as dbt models.

There are some cases where a SQL transformation is either too complex or impossible. With `fal flow` you can build your dbt models in Python.

To create a Python model, create a Python (`.py`) or [Notebook (`.ipynb`)](./notebook-files.md) file in your [`model-paths`](https://docs.getdbt.com/reference/project-configs/model-paths) and make sure your file calls the [`write_to_model`](../../Reference/variables-and-functions.md#writetomodel-function) function _exactly once_.

```py
df = ref('model_a')
df['col'] = 1

# write to data warehouse
write_to_model(df)
```

## Dependencies on other models

`fal` will resolve any usage of [`source`](../../Reference/variables-and-functions.md#source-function) or [`ref`](../../Reference/variables-and-functions.md#ref-function) functions and generate the appropriate dependencies for the `dbt` DAG.

Certain complex expressions in the Python AST may not be picked up by `fal`. In that case you can specify the dependencies in the top of the Python as a [module docstring](https://peps.python.org/pep-0257/):
```py
"""Generates Python model with forecast data

For fal to pick up these dependencies:
- ref('model_a')
- source('database_b', 'table_b')
"""

from prophet import Prophet

# fal will pick up this dependency
df_c = ref('model_c')

# calculate dataframe
df = some_calc(df_c)

# write to data warehouse
write_to_model(df)
```

## Under the hood

When running `fal flow run --experimental-models`, `fal` will automatically generate an [ephemeral dbt model](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations/#ephemeral) for each Python model. This is done in order to let `dbt` know about the existence of the Python model. This enables some nice properties such as Python models being available in `dbt` docs, and the ability to refer to (`ref`) Python models from other dbt models.

> ❗️ NOTE: Generated files should be committed to your repository.

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
The `FAL_GENERATED <checksum>` line is to make sure that the file is not being directly modified.
