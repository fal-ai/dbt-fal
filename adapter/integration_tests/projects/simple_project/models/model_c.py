from _fal_testing.utils import create_model_artifact
from fal.typing import *

def model(dbt, fal):
    dbt.config(materialized="table")
    df = dbt.ref("model_b")

    df["my_bool"] = True
    return df
