---
sidebar_position: 5
---

# Credentials and dbt profiles

`fal` integrates with `dbt`'s `profiles.yml` file to access and read data from the data warehouse. Once you setup credentials in your `profiles.yml` file for your existing `dbt` project, anytime you use `ref` or `source` to create a dataframe, `fal` authenticates using the credentials specified in the `profiles.yml` file.

## Extract-Load configuration
API configurations for some Extract-Load (EL) tools can be provided to `fal` via the `profiles.yml` file. At the moment, only two EL providers are supported: [Airbyte](https://docs.airbyte.com/) and [Fivetran](https://fivetran.com/docs/getting-started). API configurations have to be defined in a `target` node, similar to outputs. Here's an example `profiles.yml` with Airbyte and Fivetran configurations:

```yaml
fal_test:
  target: dev
  fal_extract_load:
    dev:
      my_fivetran_el:
        type: fivetran
        api_key: my_fivetran_key
        api_secret: my_fivetran_secret
        connectors:
          - name: fivetran_connector_1
            id: id_fivetran_connector_1
          - name: fivetran_connector_2
            id: id_fivetran_connector_2
      my_airbyte_el:
        type: airbyte
        host: http://localhost:8001
        connections:
          - name: airbyte_connection_1
            id: id_airbyte_connection_1
          - name: airbyte_connection_2
            id: id_airbyte_connection_2
  outputs:
    dev:
      type: postgres
      host: localhost
      user: pguser
      password: pass
      port: 5432
      dbname: test
      schema: dbt_fal
      threads: 4
```
