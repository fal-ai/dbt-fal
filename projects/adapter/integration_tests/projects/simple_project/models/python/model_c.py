def model(dbt, fal):
    from utils.get_bool import get_bool

    df = dbt.ref("model_b")

    df["my_bool"] = get_bool()
    return df
