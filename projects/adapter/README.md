<!-- <base href="https://github.com/fal-ai/fal/blob/-/projects/adapter/" target="_blank" /> -->

# Welcome to dbt-fal 👋

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
`pip install dbt-fal[bigquery, snowflake]` *Add your current warehouse here*

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

# Have feedback or need help?

- Join us in [fal on Discord](https://discord.com/invite/Fyc9PwrccF)
- Join the [dbt Community](http://community.getdbt.com/) and go into our [#tools-fal channel](https://getdbt.slack.com/archives/C02V8QW3Q4Q)
