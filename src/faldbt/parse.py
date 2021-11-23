import os
import glob
from collections import namedtuple
from pathlib import Path
from typing import List

from dbt.config.runtime import RuntimeConfig
from dbt.contracts.results import RunResultsArtifact
import dbt.tracking


class FalParseError(Exception):
    pass


RuntimeArgs = namedtuple("RuntimeArgs", "project_dir profiles_dir single_threaded")


def get_dbt_config(project_dir: str, single_threaded=False):
    from dbt.config.runtime import RuntimeConfig

    if os.getenv("DBT_PROFILES_DIR"):
        profiles_dir = os.getenv("DBT_PROFILES_DIR")
    else:
        profiles_dir = os.path.expanduser("~/.dbt")

    # Construct a phony config
    return RuntimeConfig.from_args(
        RuntimeArgs(project_dir, profiles_dir, single_threaded)
    )


def get_dbt_manifest(profiles_dir: str, config: RuntimeConfig):
    from dbt.parser.manifest import ManifestLoader

    # Necessary for ManifestLoader.get_full_manifest to not fail
    dbt.tracking.initialize_tracking(profiles_dir)

    return ManifestLoader.get_full_manifest(config)


def get_dbt_results(project_dir: str, config: RuntimeConfig) -> RunResultsArtifact:
    from dbt.exceptions import IncompatibleSchemaException, RuntimeException

    results_path = os.path.join(project_dir, config.target_path, "run_results.json")
    try:
        return RunResultsArtifact.read(results_path)
    except IncompatibleSchemaException as exc:
        exc.add_filename(results_path)
        raise
    except RuntimeException as exc:
        raise FalParseError("Did you forget to run dbt run?") from exc


def get_scripts_list(project_dir: str) -> List[Path]:
    return glob.glob(os.path.join(project_dir, "**.py"), recursive=True)
