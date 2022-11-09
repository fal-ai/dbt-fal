def model(dbt, fal):
    from get_bool import get_bool

    dbt.config(materialized="table")
    df = dbt.ref("model_b")

    df["my_bool"] = get_bool()
    return df
