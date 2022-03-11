import os
from pathlib import Path
from mock import Mock

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

    fivetran_client = faldbt._el_configs["fivetran"].get("client", None)
    assert fivetran_client is not None
    assert fivetran_client.api_key == "my_fivetran_key"
    assert fivetran_client.api_secret == "my_fivetran_secret"

    connectors = faldbt._el_configs["fivetran"].get("connectors", None)
    assert len(connectors) == 2
    assert connectors[0]["name"] is not None
    assert connectors[0]["id"] is not None

    airbyte_client = faldbt._el_configs["airbyte"].get("client", None)
    assert airbyte_client is not None
    assert airbyte_client.host == "http://localhost:8001"

    connections = faldbt._el_configs["airbyte"].get("connections", None)
    assert len(connections) == 2
    assert connections[0]["name"] is not None
    assert connections[0]["id"] is not None


def test_airbyte_sync():
    faldbt = FalDbt(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
    )
    airbyte_client = faldbt._el_configs["airbyte"].get("client", None)
    airbyte_client.sync_and_wait = Mock()

    faldbt.airbyte_sync(connection_id="id_airbyte_connection_1")
    airbyte_client.sync_and_wait.assert_called_with(
        "id_airbyte_connection_1", interval=10, timeout=None
    )

    airbyte_client.sync_and_wait.reset_mock()

    faldbt.airbyte_sync(connection_name="airbyte_connection_1")
    airbyte_client.sync_and_wait.assert_called_with(
        "id_airbyte_connection_1", interval=10, timeout=None
    )

    airbyte_client.sync_and_wait.reset_mock()

    try:
        faldbt.airbyte_sync()

    except Exception as e:
        assert str(e) == "Either connection id or connection name have to be provided."

    airbyte_client.sync_and_wait.reset_mock()

    try:
        faldbt.airbyte_sync(connection_name="not_there")
        assert False is True
    except Exception as e:
        assert (
            str(e)
            == "Couldn't find connection not_there. Did you add it to profiles.yml?"
        )


def test_fivetran_sync():
    faldbt = FalDbt(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
    )
    fivetran_client = faldbt._el_configs["fivetran"].get("client", None)
    fivetran_client.sync_and_wait = Mock()
    fivetran_client.resync_and_wait = Mock()

    faldbt.fivetran_sync(connector_id="id_fivetran_connector_1")
    fivetran_client.sync_and_wait.assert_called_with(
        "id_fivetran_connector_1", interval=10, timeout=None
    )

    fivetran_client.sync_and_wait.reset_mock()

    faldbt.fivetran_sync(connector_name="fivetran_connector_1")
    fivetran_client.sync_and_wait.assert_called_with(
        "id_fivetran_connector_1", interval=10, timeout=None
    )

    fivetran_client.sync_and_wait.reset_mock()

    try:
        faldbt.fivetran_sync()

    except Exception as e:
        assert str(e) == "Either connector id or connector name have to be provided."

    fivetran_client.sync_and_wait.reset_mock()

    try:
        faldbt.fivetran_sync(connector_name="not_there")
        assert False is True
    except Exception as e:
        assert (
            str(e)
            == "Couldn't find connector not_there. Did you add it to profiles.yml?"
        )

    faldbt.fivetran_sync(connector_name="fivetran_connector_1", historical=True)
    fivetran_client.resync_and_wait.assert_called_with(
        "id_fivetran_connector_1", interval=10, timeout=None
    )
