def model(dbt, fal):
    dbt.config(materialized="table")
    df = dbt.ref("model_b")

    df["my_bool"] = True
    return df
