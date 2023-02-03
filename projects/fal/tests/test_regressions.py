from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from fal import FalDbt

profiles_dir = os.path.join(Path.cwd(), "tests/mock/mockProfile")
project_dir = os.path.join(Path.cwd(), "tests/mock")


# https://github.com/fal-ai/fal/issues/154
def test_write_to_source_not_processing_jinja():
    faldbt = FalDbt(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
    )

    df = pd.DataFrame({"sql": [r"SELECT 1 FROM {{ wrong jinja }}"]})

    faldbt.write_to_source(df, "test_sources", "sql_col")

    # TODO: look at df data
    df = faldbt.source("test_sources", "sql_col")
    assert df.sql.get(0) == r"SELECT 1 FROM {{ wrong jinja }}"
