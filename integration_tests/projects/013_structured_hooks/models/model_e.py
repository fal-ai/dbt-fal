from _fal_testing import create_model_artifact

def model(dbt, fal):
    dbt.config(materialized='table')

    import pyjokes

    df = dbt.ref("model_c")
    df["model_e_data"] = True

    create_model_artifact(fal, f"PyJokes version: {pyjokes.__version__}")

    return df
