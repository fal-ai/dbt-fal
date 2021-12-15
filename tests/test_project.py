import os
from pathlib import Path

from fal import FalDbt

profiles_dir = os.path.join(Path.cwd(), "tests/mock/mockProfile")
project_dir = os.path.join(Path.cwd(), "tests/mock")


def test_scripts():
    faldbt = FalDbt(
        profiles_dir=profiles_dir,
        project_dir=project_dir,
    )

    assert 0 == len(faldbt._global_script_paths)

    faldbt._global_script_paths
    models = faldbt.list_models()

    # Find the correct one
    for model in models:
        if model.name == "agent_wait_time":
            assert 0 == len(model.get_scripts("fal"))
        if model.name == "zendesk_ticket_data":
            assert 1 == len(model.get_scripts("fal"))
