import pytest

from fal import FalDbt


@pytest.mark.skip("No real profiles / project directories available")
def test_initialize():
    faldbt = FalDbt(
        profiles_dir="/Users/user/.dbt",
        project_dir="/Users/user/fal/iris",
    )

    faldbt.ref("iris")
    df = faldbt.source("iris", "data")
    faldbt.write_to_source(df, "iris", "result")
    sources = faldbt.list_sources()
    models = faldbt.list_models()
