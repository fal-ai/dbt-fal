def model(dbt, session):
    dbt.config(materialized="table")

    import pandas as pd

    df_b = dbt.ref("model_b")

    if not isinstance(df_b, pd.DataFrame):
        # runs on several datawarehouses
        if hasattr(df_b, "to_pandas"):
            # Snowflake:
            df_b = df_b.to_pandas()
        if hasattr(df_b, "toPandas"):
            # PySpark:
            df_b = df_b.toPandas()

    df_b["my_bool"] = True
    res = df_b.append(df_b)

    return res
