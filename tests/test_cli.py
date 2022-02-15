from fal.cli import run_fal
import tempfile
import os
from pathlib import Path
import shutil
from dbt.exceptions import DbtProjectError

profiles_dir = os.path.join(Path.cwd(), "tests/mock/mockProfile")
project_dir = os.path.join(Path.cwd(), "tests/mock")


def test_run():
    try:
        run_fal(["fal", "run", "--profiles-dir", profiles_dir])
        assert True is False    # This line isn't reached
    except DbtProjectError as e:
        assert "no dbt_project.yml found at expected path" in str(e.msg)


def test_no_arg(capfd):
    captured = _run_fal([], capfd)
    assert "usage: fal COMMAND [<args>]" in captured.out


def test_run_with_project_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        shutil.copytree(project_dir, tmp_dir, dirs_exist_ok=True)
        run_fal(["fal", "run", "--project-dir", tmp_dir, "--profiles-dir", profiles_dir])
    assert True is True


def test_version(capfd):
    import importlib.metadata

    version = importlib.metadata.version("fal")
    captured = _run_fal(["--version"], capfd)
    assert f"fal {version}" in captured.out


def test_selection(capfd):
    with tempfile.TemporaryDirectory() as tmp_dir:
        shutil.copytree(project_dir, tmp_dir, dirs_exist_ok=True)
        captured = _run_fal([
            "run",
            "--project-dir",
            tmp_dir,
            "--profiles-dir",
            profiles_dir,
            "--select",
            "model_feature_store",
            "model_empty_scripts",
        ], capfd)

        assert "model_with_scripts" not in captured.out
        assert "model_feature_store" in captured.out
        assert "model_empty_scripts" in captured.out
        assert "model_no_fal" not in captured.out

        captured = _run_fal(
            [
                "run",
                "--project-dir",
                tmp_dir,
                "--profiles-dir",
                profiles_dir,
                "--select",
                "model_no_fal",
            ], capfd
        )
        assert "model_with_scripts" not in captured.out
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
            ["run", "--project-dir", tmp_dir, "--profiles-dir", profiles_dir],
            capfd
        )
        assert (
            "Cannot define models to run without selection flags or dbt run_results artifact"
            in str(captured.out)
        )

        # With selection flag
        captured = _run_fal(
            ["run", "--project-dir", tmp_dir, "--profiles-dir", profiles_dir, "--all"],
            capfd
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
                "--select",
                "model_with_scripts",
                "--before"
            ], capfd
        )
        assert "model_with_scripts" in captured.out
        assert "test.py" not in captured.out
        assert "model_with_before_scripts" not in captured.out
        assert "model_feature_store" not in captured.out
        assert "model_empty_scripts" not in captured.out
        assert "model_no_fal" not in captured.out

        captured = _run_fal(
            [
                "run",
                "--project-dir",
                tmp_dir,
                "--profiles-dir",
                profiles_dir,
                "--before",
            ], capfd
        )
        assert "model_with_scripts" not in captured.out
        assert "model_feature_store" not in captured.out
        assert "model_empty_scripts" not in captured.out
        assert "model_no_fal" not in captured.out
        assert "model_with_before_scripts" in captured.out


def _run_fal(args, capfd):
    # Given fal arguments, runs fal and returns capfd output
    try:
        run_fal(["fal"] + args)
    except SystemExit:
        pass
    except Exception as e:
        print(e)
    return capfd.readouterr()
