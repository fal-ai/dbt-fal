from fal.cli import DbtCliRuntimeError, cli
import tempfile
import os
from pathlib import Path
import shutil
from dbt.exceptions import DbtProjectError
import re

profiles_dir = os.path.join(Path.cwd(), "tests/mock/mockProfile")
project_dir = os.path.join(Path.cwd(), "tests/mock")


def test_run():
    try:
        cli(["fal", "run", "--profiles-dir", profiles_dir])
        assert False, "Should not reach"
    except DbtProjectError as e:
        assert "no dbt_project.yml found at expected path" in str(e.msg)


def test_flow_run():
    try:
        cli(["fal", "flow", "run", "--profiles-dir", profiles_dir])
        assert False, "Should not reach"
    except DbtCliRuntimeError as e:
        assert "Not a dbt project. Missing dbt_project.yml file" in str(e)


def test_no_arg(capfd):
    captured = _run_fal([], capfd)
    assert re.match("usage: fal (.|\n)* COMMAND", captured.err)
    assert "the following arguments are required: COMMAND" in captured.err


def test_run_with_project_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        shutil.copytree(project_dir, tmp_dir, dirs_exist_ok=True)
        cli(["fal", "run", "--project-dir", tmp_dir, "--profiles-dir", profiles_dir])


def test_version(capfd):
    import importlib.metadata

    version = importlib.metadata.version("fal")
    captured = _run_fal(["--version"], capfd)
    assert f"fal {version}" in captured.out


def test_flow_run_with_project_dir(capfd):
    with tempfile.TemporaryDirectory() as tmp_dir:
        captured = _run_fal(
            [
                "flow",
                "run",
                "--project-dir",
                tmp_dir,
                "--profiles-dir",
                profiles_dir,
            ],
            capfd,
        )

        assert re.match(
            "Executing command: dbt --log-format json run --project-dir [\w\/\-\_]+ --profiles-dir [\w\/\-\_]+tests/mock/mockProfile",
            captured.out,
        )


def test_selection(capfd):
    with tempfile.TemporaryDirectory() as tmp_dir:
        shutil.copytree(project_dir, tmp_dir, dirs_exist_ok=True)
        captured = _run_fal(
            [
                "run",
                "--project-dir",
                tmp_dir,
                "--profiles-dir",
                profiles_dir,
                "--select",
                "model_feature_store",
                "model_empty_scripts",
                "--select",
                "model_with_scripts",  # included (extend)
            ],
            capfd,
        )

        assert "model_with_scripts" not in captured.out
        assert "model_no_fal" not in captured.out
        assert (
            "Passing multiple --select/--model flags to fal is deprecatd"
            in captured.out
        )

        captured = _run_fal(
            [
                "flow",
                "run",
                "--project-dir",
                tmp_dir,
                "--profiles-dir",
                profiles_dir,
                "--select",
                "model_with_scripts",  # not included (overwrites)
                "--select",
                "model_feature_store",
                "model_empty_scripts",
            ],
            capfd,
        )

        assert "model_with_scripts" not in captured.out
        assert "model_no_fal" not in captured.out
        assert (
            "Passing multiple --select/--model flags to fal is deprecatd"
            not in captured.out
        )

        captured = _run_fal(
            [
                "run",
                "--project-dir",
                tmp_dir,
                "--profiles-dir",
                profiles_dir,
                "--select",
                "model_with_scripts",
            ],
            capfd,
        )
        assert "model_with_scripts" in captured.out
        assert "model_feature_store" not in captured.out
        assert "model_empty_scripts" not in captured.out
        # It has no keyword in meta
        assert "model_no_fal" not in captured.out


def test_no_run_results(capfd):
    with tempfile.TemporaryDirectory() as tmp_dir:
        shutil.copytree(project_dir, tmp_dir, dirs_exist_ok=True)
        shutil.rmtree(os.path.join(tmp_dir, "mockTarget"))

        # Without selection flag
        captured = _run_fal(
            ["run", "--project-dir", tmp_dir, "--profiles-dir", profiles_dir], capfd
        )
        assert (
            "Cannot define models to run without selection flags or dbt run_results artifact"
            in str(captured.out)
        )

        # With selection flag
        captured = _run_fal(
            ["run", "--project-dir", tmp_dir, "--profiles-dir", profiles_dir, "--all"],
            capfd,
        )

        # Just as warning
        assert "Could not read dbt run_results artifact" in captured.out


def test_before(capfd):
    with tempfile.TemporaryDirectory() as tmp_dir:
        shutil.copytree(project_dir, tmp_dir, dirs_exist_ok=True)

        captured = _run_fal(
            [
                "run",
                "--project-dir",
                tmp_dir,
                "--profiles-dir",
                profiles_dir,
                "--before",
            ],
            capfd,
        )
        assert "model_with_scripts" not in captured.out
        assert "model_feature_store" not in captured.out
        assert "model_empty_scripts" not in captured.out
        assert "model_no_fal" not in captured.out
        assert "model_with_before_scripts" in captured.out


def _run_fal(args, capfd):
    # Given fal arguments, runs fal and returns capfd output
    try:
        cli(["fal"] + args)
    except SystemExit:
        pass
    except Exception as e:
        print(e)
    return capfd.readouterr()
