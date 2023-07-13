import os
from pathlib import Path
from mock import Mock, patch, ANY
import requests

from fal.dbt import FalDbt

profiles_dir = os.path.join(Path.cwd(), "tests/mock/mockProfile")
project_dir = os.path.join(Path.cwd(), "tests/mock")


def test_scripts():
    faldbt = FalDbt(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
    )

    assert isinstance(faldbt._global_script_paths, dict)
    assert 0 == len(faldbt._global_script_paths["after"])

    faldbt._global_script_paths
    models = faldbt.list_models()

    # Find the correct one
    for model in models:
        if model.name == "model_feature_store":
            assert 0 == len(model.get_scripts(before=False))
        if model.name == "model_with_scripts":
            assert 1 == len(model.get_scripts(before=False))
            assert 0 == len(model.get_scripts(before=True))
        if model.name == "model_with_before_scripts":
            assert 1 == len(model.get_scripts(before=True))
            assert 0 == len(model.get_scripts(before=False))


def test_features():
    faldbt = FalDbt(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
    )

    # Feature definitions
    features = faldbt.list_features()

    assert 1 == len(features)
    fs_def = features[0]
    assert "a" == fs_def.entity_column
    assert "model_feature_store" == fs_def.model
