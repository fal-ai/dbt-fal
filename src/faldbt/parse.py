import os
from dataclasses import dataclass
import glob
from pathlib import Path
from typing import List, Dict, Optional, Union

from dbt.contracts.project import Project as ProjectContract
from dbt.config import RuntimeConfig, Project
from dbt.config.utils import parse_cli_vars
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunResultsArtifact, FreshnessExecutionResultArtifact
from dbt.contracts.project import UserConfig
from dbt.config.profile import read_user_config
from dbt.exceptions import IncompatibleSchemaException, RuntimeException
from dbt.logger import GLOBAL_LOGGER as logger

from faldbt.utils.yaml_helper import load_yaml

FAL_SCRIPTS_PATH = "fal-scripts-path"


class FalParseError(Exception):
    pass


def get_dbt_user_config(profiles_dir: str) -> UserConfig:
    return read_user_config(profiles_dir)


@dataclass
class RuntimeArgs:
    project_dir: str
    profiles_dir: str
    threads: Optional[int]
    single_threaded: bool
    profile: Optional[str]
    target: Optional[str]


def load_dbt_project_contract(project_dir: str) -> ProjectContract:
    partial_project = Project.partial_load(project_dir)
    contract = ProjectContract.from_dict(partial_project.project_dict)
    if not hasattr(contract, "model_paths") or contract.model_paths is None:
        setattr(contract, "model_paths", contract.source_paths)
    if not hasattr(contract, "seed_paths") or contract.seed_paths is None:
        setattr(contract, "seed_paths", contract.data_paths)
    return contract


def get_dbt_config(
    *,
    project_dir: str,
    profiles_dir: str,
    profile_target: Optional[str] = None,
    threads: Optional[int] = None,
    profile: Optional[str] = None,
) -> RuntimeConfig:
    # Construct a phony config
    args = RuntimeArgs(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        threads=threads,
        single_threaded=False,
        profile=profile,
        target=profile_target,
    )
    return RuntimeConfig.from_args(args)


def get_el_configs(
    profiles_dir: str, profile_name: str, target_name: str
) -> Dict[str, Dict]:
    path = os.path.join(profiles_dir, "profiles.yml")
    yml = load_yaml(path)
    sync_configs = (
        yml.get(profile_name, {}).get("fal_extract_load", {}).get(target_name, {})
    )
    return sync_configs


def get_scripts_dir(project_dir: str, args_vars: str) -> str:
    project_contract = load_dbt_project_contract(project_dir)

    # This happens inside unit tests usually
    if project_contract is None:
        return project_dir

    cli_vars = parse_cli_vars(args_vars)
    scripts_dir = cli_vars.get(FAL_SCRIPTS_PATH, None)

    if scripts_dir is None:
        vars = project_contract.vars or {}
        scripts_dir = vars.get(FAL_SCRIPTS_PATH, project_dir)

    if not isinstance(scripts_dir, str):
        raise FalParseError("Error parsing scripts_dir")

    return os.path.join(project_dir, scripts_dir)


def get_dbt_manifest(config) -> Manifest:
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.get_full_manifest(config)


def get_dbt_sources_artifact(project_dir: str, config: RuntimeConfig):
    sources_path = os.path.join(project_dir, config.target_path, "sources.json")
    try:
        # BACKWARDS: Change intorduced in 1.0.0
        if hasattr(FreshnessExecutionResultArtifact, "read_and_check_versions"):
            return FreshnessExecutionResultArtifact.read_and_check_versions(
                sources_path
            )
        else:
            return FreshnessExecutionResultArtifact.read(sources_path)

    except IncompatibleSchemaException as exc:
        # TODO: add test for this case
        exc.add_filename(sources_path)
        raise
    except RuntimeException as exc:
        logger.warn("Could not read dbt sources artifact")
        return None


def get_dbt_results(
    project_dir: str, config: RuntimeConfig
) -> Optional[RunResultsArtifact]:
    results_path = os.path.join(project_dir, config.target_path, "run_results.json")
    try:
        # BACKWARDS: Change intorduced in 1.0.0
        if hasattr(RunResultsArtifact, "read_and_check_versions"):
            return RunResultsArtifact.read_and_check_versions(results_path)
        else:
            return RunResultsArtifact.read(results_path)

    except IncompatibleSchemaException as exc:
        # TODO: add test for this case
        exc.add_filename(results_path)
        raise
    except RuntimeException as exc:
        logger.warn("Could not read dbt run_results artifact")
        return None


def get_scripts_list(scripts_dir: str) -> List[str]:
    scripts_path = Path(scripts_dir)
    return list(map(str, [*scripts_path.rglob("*.py"), *scripts_path.rglob("*.ipynb")]))


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
                raise FalParseError("Error parsing the schema file " + file)

    return global_scripts


def normalize_path(base: str, path: Union[Path, str]):
    real_base = os.path.realpath(os.path.normpath(base))
    return Path(os.path.realpath(os.path.join(real_base, path)))


def normalize_paths(
    base: str, paths: Union[List[Path], List[str], List[Union[Path, str]]]
):
    return list(map(lambda path: normalize_path(base, path), paths))
