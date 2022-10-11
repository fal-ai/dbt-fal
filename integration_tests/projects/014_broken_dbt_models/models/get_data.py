from _fal_testing import create_model_artifact

def model(dbt, fal):
    dbt.config(materialized='table')

    import pandas as pd
    df = pd.DataFrame()

    df["negative_data"] = [
        -1,
        -2,
        -3,
    ]
    df["positive_data"] = [
        1,
        2,
        3,
    ]

    create_model_artifact(fal)

    return df
