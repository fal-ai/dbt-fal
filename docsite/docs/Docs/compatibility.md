---
sidebar_position: 8
---

# Compatibility

## Picking the right fal version

If you are starting a new project, we'd recommend you to use the latest fal
version in order to gain access to all the features fal is offering. In the
case of adoption to an existing project, you can use the tables below for
picking the right version that would work best for your environment

#### fal <> dbt matrix

|     | fal version | Supported dbt version        |
| --- | ----------- | ---------------------------- |
|     | 0.4.0>=     | 1.0.X, 1.1.X                 |
|     | 0.3.6\<=    | 0.20.X, 0.21.X, 1.0.X, 1.1.X |

#### fal <> dbt adapter matrix

|     | fal version | Supported dbt adapters                          | Notes                                                                                 |
| --- | ----------- | ----------------------------------------------- | ------------------------------------------------------------------------------------- |
|     | 0.3.6>=     | Postgres, BigQuery, Snowflake, Redshift, DuckDB | dbt-bigquery\<=1.1 support is added  ([#443](https://github.com/fal-ai/fal/pull/443)) |
|     | 0.3.1>=     | Postgres, BigQuery, Snowflake, Redshift, DuckDB |                                                                                       |
|     | 0.3.0\<=    | Postgres, BigQuery, Snowflake, Redshift         |                                                                                       |

## Migration

### From `after` scripts to `post-hook`s

With the [`0.4.0`](https://github.com/fal-ai/fal/releases/tag/v0.4.0) release,
fal will start showing deprecation warnings for `after` scripts when using
`fal flow run`. The easiest way forward is migrating them to `post-hook`s, and
in general the migration is seamless. But if any of the scripts you have is
using `write_to_source` or `write_to_model` functions, we'd recommend promoting
them to individual ["Python models"](/Guides/python-models-migration).
