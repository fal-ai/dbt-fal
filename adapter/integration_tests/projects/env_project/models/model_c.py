def model(dbt, fal):
    dbt.config(materialized="table")
    dbt.config(fal_environment="funny")
    import pyjokes

    joke = pyjokes.get_joke()
    df = dbt.ref("model_a")

    df["my_joke"] = joke
    return df
