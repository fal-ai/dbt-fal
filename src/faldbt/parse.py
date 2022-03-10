import os
from dataclasses import dataclass
import glob
from pathlib import Path
from typing import List, Dict, Union

from dbt.config import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunResultsArtifact
from dbt.contracts.project import UserConfig
from dbt.config.profile import read_user_config
from dbt.exceptions import IncompatibleSchemaException, RuntimeException
from dbt.logger import GLOBAL_LOGGER as logger
from fal.el.airbyte import AirbyteClient
from fal.el.fivetran import FivetranClient

from faldbt.utils.yaml_helper import load_yaml


class FalParseError(Exception):
    pass


def get_dbt_user_config(profiles_dir: str) -> UserConfig:
    return read_user_config(profiles_dir)


@dataclass
class RuntimeArgs:
    project_dir: str
    profiles_dir: str
    threads: Union[int, None]
    single_threaded: bool


def get_dbt_config(
    project_dir: str, profiles_dir: str, threads: Union[int, None] = None
) -> RuntimeConfig:
    # Construct a phony config
    args = RuntimeArgs(project_dir, profiles_dir, threads, single_threaded=False)
    return RuntimeConfig.from_args(args)


def get_el_adapters(profiles_dir: str, profile_name: str):
    path = os.path.join(profiles_dir, "profiles.yml")
    yml = load_yaml(path)
    adapters = yml.get(profile_name, {}).get("fal", {}).get("el", [])
    adapter_map = {"fivetran": {}, "airbyte": {}}
    for adapter in adapters:
        if adapter["type"] == "fivetran":
            adapter_map["fivetran"]["client"] = FivetranClient(
                api_key=adapter["api_key"],
                api_secret=adapter["api_secret"],
                disable_schedule_on_trigger=adapter.get(
                    "disable_schedule_trigger", None
                ),
                max_retries=adapter.get("max_retries", 3),
                retry_delay=adapter.get("retry_delay", 0.25),
            )
            adapter_map["fivetran"]["connectors"] = adapter.get("connectors", [])
        elif adapter["type"] == "airbyte":
            adapter_map["airbyte"]["client"] = AirbyteClient(
                host=adapter["host"],
                max_retries=adapter.get("max_retries", 5),
                retry_delay=adapter.get("retry_delay", 5),
            )
            adapter_map["airbyte"]["connections"] = adapter.get("connections", [])
    return adapters


def get_dbt_manifest(config) -> Manifest:
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.get_full_manifest(config)


def get_dbt_results(project_dir: str, config: RuntimeConfig) -> RunResultsArtifact:
    results_path = os.path.join(project_dir, config.target_path, "run_results.json")
    try:
        # BACKWARDS: Change intorduced in 1.0.0
        if hasattr(RunResultsArtifact, "read_and_check_versions"):
            return RunResultsArtifact.read_and_check_versions(results_path)
        else:
            return RunResultsArtifact.read(results_path)
    except IncompatibleSchemaException as exc:
        exc.add_filename(results_path)
        raise
    except RuntimeException as exc:
        logger.warn("Could not read dbt run_results artifact")
        return None


def get_scripts_list(project_dir: str) -> List[str]:
    return glob.glob(os.path.join(project_dir, "**.py"), recursive=True)


def get_global_script_configs(source_dirs: List[Path]) -> Dict[str, List[str]]:
    global_scripts = {"before": [], "after": []}
    for source_dir in source_dirs:
        schema_files = glob.glob(os.path.join(source_dir, "**.yml"), recursive=True)
        for file in schema_files:
            schema_yml = load_yaml(file)
            if schema_yml is not None:
                fal_config = schema_yml.get("fal", None)
                if fal_config is not None:
                    # sometimes `scripts` can *be* there and still be None
                    script_paths = fal_config.get("scripts") or []
                    if isinstance(script_paths, list):
                        global_scripts["after"] += script_paths
                    else:
                        global_scripts["before"] += script_paths.get("before") or []
                        global_scripts["after"] += script_paths.get("after") or []
            else:
                raise FalParseError("Error pasing the schema file " + file)

    return global_scripts
