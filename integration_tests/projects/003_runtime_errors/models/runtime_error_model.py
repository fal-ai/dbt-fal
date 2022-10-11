def model(dbt, fal):
    dbt.config(materialized='table')

    df = dbt.ref("working_model")

    assert False, "expected"

    return df
