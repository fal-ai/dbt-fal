from _fal_testing import create_model_artifact

def model(dbt, fal):
    dbt.config(materialized='table')

    df = dbt.ref("model_a1")
    df["b1_data"] = 1

    create_model_artifact(fal)

    return df

