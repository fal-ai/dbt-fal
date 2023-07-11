from fal import FalDbt
from pathlib import Path
import os
import pandas as pd

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

    # The sql string should not be processed by jinja and written as is
    df = faldbt.source("test_sources", "sql_col")
    assert df.sql.get(0) == r"SELECT 1 FROM {{ wrong jinja }}"
