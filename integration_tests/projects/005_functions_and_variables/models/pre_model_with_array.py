from _fal_testing.utils import create_model_artifact

def model(dbt, fal):
    dbt.config(materialized='table')

    import pandas as pd

    df = pd.DataFrame(
        {
            "my_array": [["some", "other"], []],
            "other_array": [[1, 2, 3], []],
        }
    )
    df.info()

    output = f"my_array: {list(df['my_array'][0])}"
    output += f"\nother_array: {list(df['other_array'][0])}"

    create_model_artifact(fal, additional_data=output)
    return df
