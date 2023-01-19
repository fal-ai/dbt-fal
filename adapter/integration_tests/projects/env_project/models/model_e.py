def model(dbt, fal):
    dbt.config(materialized="table")
    dbt.config(fal_environment="funny-conda")
    import pyjokes
    from utils.get_bool import get_bool

    joke = pyjokes.get_joke()
    df = dbt.ref("model_a")

    df["my_joke"] = joke
    df["my_bool"] = get_bool()
    return df
