from fal import FalDbt
from pathlib import Path
import os


profiles_dir = os.path.join(Path.cwd(), "tests/mock/mockProfile")
project_dir = os.path.join(Path.cwd(), "tests/mock")


def test_initialize():
    faldbt = FalDbt(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
    )

    # TODO: look at df data
    df = faldbt.ref("model_with_scripts")

    faldbt.write_to_model(df, "model_with_scripts", mode="append")

    faldbt.write_to_source(df, "test_sources", "single_col")

    # TODO: look at df data
    df = faldbt.source("test_sources", "single_col")

    # TODO: look at df data (should be double now)
    df = faldbt.ref("model_with_scripts")

    sources = faldbt.list_sources()
    assert ["test_sources", "single_col"] in sources

    models = faldbt.list_models()
    assert "model_with_scripts" in [model.name for model in models]
