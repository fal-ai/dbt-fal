from fal import __version__

from click.testing import CliRunner
from fal.cli import run
import tempfile
import os
from pathlib import Path
import shutil

# def test_missing_arg():
#     runner = CliRunner()
#     result = runner.invoke(run, [])
#     assert result.exit_code == 2
#     assert "Error: Missing argument 'RUN'" in result.output

# def test_run():
#     runner = CliRunner()
#     result = runner.invoke(run, ["run"])
#     assert "no dbt_project.yml found at expected path" in result.exception

# def test_run():
#     runner = CliRunner()
#     with tempfile.TemporaryDirectory() as tmp_dir:
#         shutil.copytree(os.path.join(Path.cwd(), "tests/mock"), tmp_dir, dirs_exist_ok=True)
#         result = runner.invoke(run, ["run", "--project-dir", tmp_dir])
#     assert result.exit_code == 0

def test_version():
    runner = CliRunner()
    result = runner.invoke(run, ["run", "--project-dir", "/Users/gorkemyurtseven/dbt_project/fal_dbt_examples"])

    assert __version__ == '0.1.0'


