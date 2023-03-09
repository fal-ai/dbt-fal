---
sidebar_position: 5
---

# Credentials and dbt profiles

`fal` integrates with `dbt`'s `profiles.yml` file to access and read data from the data warehouse. Once you setup credentials in your `profiles.yml` file for your existing `dbt` project, anytime you use `ref` or `source` to create a dataframe, `fal` authenticates using the credentials specified in the `profiles.yml` file.
