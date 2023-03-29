---
sidebar_position: 2
---

# Environment management with dbt-fal

Our recommended way of using dbt-fal is to use named environments. They help you define reusable Python environments that are automatically managed by dbt-fal. You can use them by creating a `fal_project.yml` file in the same folder as your dbt project, and then use these environments in any Python model.

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
