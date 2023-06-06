def model(dbt, fal):
    from utils.get_bool import get_bool
    import json

    dbt.config(materialized="table")

    df = dbt.ref("model_b")

    df["my_bool"] = get_bool()
    df["my_json"] = json.dumps({"a": 1, "b": 2})
    return df
