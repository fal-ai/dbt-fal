import os
import json
import glob, os

import click

from faldbt.utils.yaml_helper import load_yaml_text
from faldbt.project import (
    DbtModel,
    DbtProject,
    DbtManifest,
    DbtRunResultFile,
)

from typing import Dict, List, List, Any
from pathlib import Path


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


def _model_from_path(config_path) -> List[DbtModel]:
    config_dict = _load_yaml(config_path)
    return list(map(lambda model: DbtModel(**model), config_dict["models"]))


def _get_all_model_config(project_root, project_dict):
    return _flatten(
        map(
            ## find one with any kind yml this doesnt need to schema
            ## look at all of them find the ones that has model in them
            ## and keep remembering it
            lambda model_path: glob.glob(
                os.path.join(project_root, model_path + "/**/*.yml"), recursive=True
            ),
            project_dict["source-paths"],
        )
    )


def parse_project(root_dir, keyword):
    project_root = os.path.normpath(root_dir)
    project_yaml_filepath = os.path.join(project_root, "dbt_project.yml")

    scripts = glob.glob(os.path.join(project_root, project_root + "/**.py"))

    if not os.path.lexists(project_yaml_filepath):
        raise FalParseError(
            "no dbt_project.yml found at expected path {}".format(project_yaml_filepath)
        )

    project_dict = _load_yaml(project_yaml_filepath)

    if not isinstance(project_dict, dict):
        raise FalParseError("dbt_project.yml does not parse to a dictionary")

    model_config_paths = _get_all_model_config(project_root, project_dict)
    models = list(
        map(lambda config_path: _model_from_path(config_path), model_config_paths)
    )

    target_path = os.path.join(project_root, project_dict["target-path"])
    manifest_path = os.path.join(target_path, "manifest.json")
    run_result_path = os.path.join(target_path, "run_results.json")
    results = DbtRunResultFile(**_read_json(run_result_path))

    return DbtProject(
        name=project_dict["name"],
        model_config_paths=list(model_config_paths),
        models=_flatten(models),
        manifest=parse_manifest(manifest_path),
        keyword=keyword,
        scripts=scripts,
        results=results,
    )


def parse_manifest(manifest_path) -> DbtManifest:
    manifest_data = _read_json(manifest_path)
    return DbtManifest(**manifest_data)

