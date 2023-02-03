from __future__ import annotations


def model(dbt, fal):
    import pyjokes

    dbt.config(materialized="table")

    joke = pyjokes.get_joke()
    df = dbt.ref("model_a")

    df["my_joke"] = joke
    return df
