import os
import json
import glob
from dbt.config.runtime import RuntimeConfig
import dbt.tracking
import faldbt.lib as lib
from dbt.contracts.results import RunResultsArtifact
from faldbt.utils.yaml_helper import load_yaml_text
from typing import Dict, Any
from faldbt.project import DbtProject, DbtManifest, DbtRunResult


class FalParseError(Exception):
    pass


def _load_file_contents(path: str, strip: bool = True) -> str:
    with open(path, "rb") as handle:
        to_return = handle.read().decode("utf-8")

    if strip:
        to_return = to_return.strip()

    return to_return


def _load_yaml(path):
    contents = _load_file_contents(path)
    return load_yaml_text(contents)


def _read_json(path: str) -> Dict[str, Any]:
    return json.loads(_load_file_contents(path))


def _flatten(t):
    return [item for sublist in t for item in sublist]


def _get_all_model_config(project_root, project_dict):
    return _flatten(
        map(
            ## find one with any kind yml this doesnt need to schema
            ## look at all of them find the ones that has model in them
            ## and keep remembering it
            lambda model_path: glob.glob(
                os.path.join(project_root, model_path, "**.yml"), recursive=True
            ),
            project_dict["source-paths"],
        )
    )


def parse_project(project_dir: str, profiles_dir: str, keyword: str):
    project_dict = _get_project_dict(project_dir)
    scripts = glob.glob(os.path.join(project_dir, "**.py"), recursive=True)
    model_config_paths = _get_all_model_config(project_dir, project_dict)
    target_path = os.path.join(project_dir, project_dict["target-path"])
    run_result_path = os.path.join(target_path, "run_results.json")

    config = lib.get_dbt_config(project_dir)
    lib.register_adapters(config)

    # Necessary for parse_to_manifest to not fail
    dbt.tracking.initialize_tracking(profiles_dir)

    manifest = lib.parse_to_manifest(config)
    run_result_artifact = RunResultsArtifact(**_read_json(run_result_path))
    dbtmanifest = DbtManifest(nativeManifest=manifest)

    models = dbtmanifest.get_models()
    status_map = dict(
        map(lambda result: [result["unique_id"], result["status"]], run_result_artifact)
    )
    for model in models:
        model.status = status_map[model.unique_id]

    return DbtProject(
        name=project_dict["name"],
        model_config_paths=list(model_config_paths),
        models=models,
        manifest=DbtManifest(nativeManifest=manifest),
        keyword=keyword,
        scripts=scripts,
        run_result=DbtRunResult(run_result_artifact),
    )


def _get_project_dict(project_dir):
    project_yaml_filepath = os.path.join(project_dir, "dbt_project.yml")

    if not os.path.lexists(project_yaml_filepath):
        raise FalParseError(
            "no dbt_project.yml found at expected path {}".format(project_yaml_filepath)
        )

    project_dict = _load_yaml(project_yaml_filepath)

    if not isinstance(project_dict, dict):
        raise FalParseError("dbt_project.yml does not parse to a dictionary")

    return project_dict
