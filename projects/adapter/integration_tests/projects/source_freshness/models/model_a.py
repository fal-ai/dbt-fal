from __future__ import annotations


def model(dbt, fal):
    dbt.config(materialized="table")
    dbt.config(fal_environment="funny")
    import pyjokes

    joke = pyjokes.get_joke()
    df = dbt.source("freshness_test", "freshness_table")
    df["my_joke"] = joke
    return df
