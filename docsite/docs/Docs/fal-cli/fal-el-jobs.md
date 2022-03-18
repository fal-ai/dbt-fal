---
sidebar_position: 6
---

# Run Extract-Load jobs
`fal` makes it easy to run EL jobs on your data pipelines. Two common types of EL pipeline providers are supported: [Airbyte](https://docs.airbyte.com/) and [Fivetran](https://fivetran.com/docs/getting-started). More providers will be available in near future.

In order to be run EL jobs, you first need to setup EL configurations in [`profiles.yml`](../credentials-profile#exctract-load-configuration).

Here's how you trigger an Airbyte job:

```python
el.airbyte_sync(config_name="my_airbyte_el", connection_name="airbyte_connection_1")
```
where `el` is a magic variable that doesn't need to be imported, `config_name` is the name of the configuration tag that you set in `profiles.yml`, and `connection_name` is the name of the individual Airbyte connection that you want to sync.

Similarly, this is how you run a Fivetran job:

```python
el.fivetran_sync(config_name="my_fivetran_el", connector_name="fivetran_connector_1")
```
Note that unlike Airbyte, you sync *connectors* in Fivetran and therefore you need to pass a `connector_name`.

For more information on these functions see [here](../../Reference/variables-and-functions#airbyte_sync-and-fivetran_sync-functions).
