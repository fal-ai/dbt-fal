from _fal_testing.utils import create_model_artifact

def model(dbt, fal):
    dbt.config(materialized='table')

    df = dbt.ref('pre_model_with_array')
    df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
    df.info()

    output = f"my_array: {list(df['my_array'][0])}"
    output += f"\nother_array: {list(df['other_array'][0])}"
    create_model_artifact(fal, additional_data=output)

    return df
