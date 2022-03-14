import os
from pathlib import Path
from mock import Mock, patch, ANY
import requests

from fal import FalDbt

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
            assert 0 == len(model.get_scripts("fal", before=False))
        if model.name == "model_with_scripts":
            assert 1 == len(model.get_scripts("fal", before=False))
            assert 0 == len(model.get_scripts("fal", before=True))
        if model.name == "model_with_before_scripts":
            assert 1 == len(model.get_scripts("fal", before=True))
            assert 0 == len(model.get_scripts("fal", before=False))


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


def test_adapter_setup():
    faldbt = FalDbt(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
    )

    config_list = faldbt._el_configs
    assert len(config_list) == 2

    fivetran_config = config_list[0]

    assert fivetran_config["name"] == "my_fivetran_el"
    assert len(fivetran_config["connectors"]) == 2
    assert fivetran_config["connectors"][0]["name"] == "fivetran_connector_1"

    airbyte_config = config_list[1]

    assert airbyte_config["name"] == "my_airbyte_el"
    assert len(airbyte_config["connections"]) == 2
    assert airbyte_config["connections"][0]["name"] == "airbyte_connection_1"


def test_airbyte_sync():
    faldbt = FalDbt(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
    )
    with patch("fal.el.airbyte.AirbyteClient.sync_and_wait") as mock_sync:
        faldbt.airbyte_sync(
            config_name="my_airbyte_el", connection_id="id_airbyte_connection_1"
        )

        mock_sync.assert_called_with("id_airbyte_connection_1")

        mock_sync.reset_mock()

        faldbt.airbyte_sync(
            config_name="my_airbyte_el", connection_name="airbyte_connection_1"
        )

        mock_sync.assert_called_with("id_airbyte_connection_1")

        try:
            faldbt.airbyte_sync(
                config_name="not_there", connection_name="airbyte_connection_1"
            )

        except Exception as e:
            assert str(e) == "EL configuration not_there is not found."

        try:
            faldbt.airbyte_sync(
                config_name="my_airbyte_el", connection_name="not_there"
            )

        except Exception as e:
            assert str(e) == "Connection not_there not found."

        try:
            faldbt.airbyte_sync(config_name="my_airbyte_el")

        except Exception as e:
            assert (
                str(e) == "Either connection id or connection name have to be provided."
            )


def test_fivetran_sync():
    faldbt = FalDbt(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
    )
    with patch("fal.el.fivetran.FivetranClient.sync_and_wait") as mock_sync:
        faldbt.airbyte_sync(
            config_name="my_fivetran_el", connection_id="id_fivetran_connection_1"
        )

        mock_sync.assert_called_with(connector_id="id_fivetran_connection_1")

        mock_sync.reset_mock()

        faldbt.fivetran_sync(
            config_name="my_fivetran_el", connector_name="fivetran_connector_1"
        )

        mock_sync.assert_called_with(connector_id="id_fivetran_connector_1")

        try:
            faldbt.fivetran_sync(
                config_name="not_there", connector_name="fivetran_connector_1"
            )

        except Exception as e:
            assert str(e) == "EL configuration not_there is not found."

        try:
            faldbt.fivetran_sync(
                config_name="my_fivetran_el", connector_name="not_there"
            )

        except Exception as e:
            assert str(e) == "Connection not_there not found."

        try:
            faldbt.fivetran_sync(config_name="my_fivetran_el")

        except Exception as e:
            assert (
                str(e) == "Either connection id or connection name have to be provided."
            )
