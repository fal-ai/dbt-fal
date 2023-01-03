---
sidebar_position: 1
---

# Quickstart
dbt-fal adapter is the ✨easiest✨ way to run your [dbt Python models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/python-models).

Starting with dbt v1.3, you can now build your dbt models in Python. dbt-fal provides the best environment to run your Python models that works with all other data warehouses! With dbt-fal, you can:

- Build and test your models locally
- Isolate each model to run in its own environment with its own dependencies
- [Coming Soon] Run your Python models in the ☁️ cloud ☁️ with elasticly scaling Python environments.
- [Coming Soon] Even add GPUs to your models for some heavier workloads such as training ML models.

## 1. Install dbt-fal
`pip install dbt-fal[bigquery, snowflake]` *Add your current warehouse here*

## 2. Update your `profiles.yml` and add the fal adapter
Add another entry to `outputs` in your desired profile (below we've added `dev_with_fal`)

```yaml
jaffle_shop:
  target: dev_with_fal # target points at the new output
  outputs:
    dev_bigquery:
      type: bigquery
      method: service-account
      keyfile: /path/to/keyfile.json
      project: my_gcp_project
      dataset: my_dbt_dataset
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
    dev_with_fal: # Name of your new output
      type: fal
      db_profile: dev_bigquery # This points to your main adapter
```

Don't forget to point to your main adapter with the `db_profile` attribute. This is how the fal adapter knows how to connect to your data warehouse.

## 3. Run dbt

```bash
dbt run
```

That is it! It is really that simple 😊

## 4. Environment management with dbt-fal
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
