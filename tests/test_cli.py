from click.testing import CliRunner
from fal.cli import cli
import tempfile
import os
from pathlib import Path
import shutil
import pytest

profiles_dir = os.path.join(Path.cwd(), "tests/mock/mockProfile")


def test_run():
    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--profiles-dir", profiles_dir])
    assert "no dbt_project.yml found at expected path" in str(result.exception)


def test_no_arg():
    runner = CliRunner()
    result = runner.invoke(cli, [])
    assert result.exit_code == 0
    assert "Usage: cli [OPTIONS] COMMAND [ARGS]..." in result.output


def test_run_with_project_dir():
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmp_dir:
        shutil.copytree(
            os.path.join(Path.cwd(), "tests/mock"), tmp_dir, dirs_exist_ok=True
        )
        result = runner.invoke(
            cli, ["run", "--project-dir", tmp_dir, "--profiles-dir", profiles_dir]
        )
    assert result.exit_code == 0


def test_version():
    import importlib.metadata

    version = importlib.metadata.version("fal")

    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert f"cli, version {version}" in result.output


def test_selection(capfd):
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmp_dir:
        shutil.copytree(
            os.path.join(Path.cwd(), "tests/mock"), tmp_dir, dirs_exist_ok=True
        )
        result = runner.invoke(
            cli,
            [
                "run",
                "--project-dir",
                tmp_dir,
                "--profiles-dir",
                profiles_dir,
                "--select",
                "agent_wait_time",
            ],
        )

    captured = capfd.readouterr()
    assert result.exit_code == 0
    assert "agent_wait_time: " in captured.out
    assert "zendesk_ticket_data" not in captured.out
