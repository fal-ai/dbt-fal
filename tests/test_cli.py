from fal import __version__

from click.testing import CliRunner
from fal.cli import cli
import tempfile
import os
from pathlib import Path
import shutil
import pytest


def test_run():
    runner = CliRunner()
    result = runner.invoke(cli, ["run"])
    assert "no dbt_project.yml found at expected path" in str(result.exception)


def test_no_arg():
    runner = CliRunner()
    result = runner.invoke(cli, [])
    assert result.exit_code == 0
    assert "Usage: cli [OPTIONS] COMMAND [ARGS]..." in result.output


@pytest.mark.skip("Getting 'Could not find profile named 'fal_dbt_examples''")
def test_run_with_project_dir():
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmp_dir:
        shutil.copytree(
            os.path.join(Path.cwd(), "tests/mock"), tmp_dir, dirs_exist_ok=True
        )
        result = runner.invoke(cli, ["run", "--project-dir", tmp_dir])
    assert result.exit_code == 0


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert f"cli, version {__version__}" in result.output
