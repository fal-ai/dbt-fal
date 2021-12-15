import os
from collections import namedtuple
import json
import glob
from pathlib import Path
from typing import List

from dbt.config import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunResultsArtifact
from faldbt.utils.yaml_helper import load_yaml


class FalParseError(Exception):
    pass


RuntimeArgs = namedtuple("RuntimeArgs", "project_dir profiles_dir single_threaded")


def get_dbt_config(
    project_dir: str, profiles_dir: str, single_threaded=False
) -> RuntimeConfig:

    # Construct a phony config
    return RuntimeConfig.from_args(
        RuntimeArgs(project_dir, profiles_dir, single_threaded)
    )


def get_dbt_manifest(config) -> Manifest:
    from dbt.parser.manifest import ManifestLoader

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


def get_scripts_list(project_dir: str) -> List[str]:
    return glob.glob(os.path.join(project_dir, "**.py"), recursive=True)


def get_global_script_configs(source_dirs: List[Path]) -> List[str]:
    global_scripts = []
    for source_dir in source_dirs:
        schema_files = glob.glob(os.path.join(source_dir, "**.yml"), recursive=True)
        for file in schema_files:
            schema_yml = load_yaml(file)
            if schema_yml is not None:
                fal_config = schema_yml.get("fal", None)
                if fal_config is not None:
                    # sometimes `scripts` can *be* there and still be None
                    script_paths = fal_config.get("scripts") or []
                    global_scripts += script_paths
            else:
                raise FalParseError("Error pasing the schema file " + file)

    return global_scripts
