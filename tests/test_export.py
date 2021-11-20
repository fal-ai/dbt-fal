from fal import FalDbt


def test_initialize():
    faldbt = FalDbt(
        profiles_dir="/Users/matteo/.dbt",
        project_dir="/Users/matteo/Projects/fal/experiments/iris",
    )
    faldbt.ref("iris")
    df = faldbt.source("iris", "data")
    faldbt.write_to_source(df, "iris", "result")
