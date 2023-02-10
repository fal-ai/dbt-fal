---
sidebar_position: 1
---

# Quickstart

dbt-fal adapter is the ✨easiest✨ way to run your [dbt Python models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/python-models).

Starting with dbt v1.3, you can now build your dbt models in Python. However the developer experience with existing datawarehouse Python runtimes is not ideal.

dbt-fal provides the best environment to run your Python models that works with all other data warehouses! This includes Postgres, Redshift which do not have Python support, as well as Bigquery, Snowflake which are too hard to work with.

With dbt-fal, you can:

- Build and test your models locally
- Isolate each model to run in its own environment with its own dependencies
- Run your Python models in the [☁️ cloud ☁️](cloud/using_fal_cloud) with elasticly scaling Python environments and pay for only what you use.
- Even add GPUs to your models for some heavy workloads such as training ML models. 🤖

## 1. Install dbt-fal

`pip install dbt-fal[bigquery, snowflake]` _Add your current warehouse here_

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
