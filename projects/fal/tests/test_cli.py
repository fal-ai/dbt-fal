import inspect
import os
import re
import shutil
import tempfile
import pytest
from pathlib import Path

from dbt.exceptions import DbtProjectError
from fal.cli import cli

profiles_dir = os.path.join(Path.cwd(), "tests/mock/mockProfile")
project_dir = os.path.join(Path.cwd(), "tests/mock")


class ProjectTemporaryDirectory(tempfile.TemporaryDirectory):
    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)

        # Copy project_dir to a clean temp directory
        shutil.rmtree(self.name, ignore_errors=True)
        shutil.copytree(project_dir, self.name)


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
    except DbtProjectError as e:
        assert "no dbt_project.yml found at expected path" in str(e)


def test_no_arg(capfd):
    captured = _run_fal([], capfd)
    assert re.match("usage: fal (.|\n)* COMMAND", captured.err)
    assert "the following arguments are required: COMMAND" in captured.err


def test_run_with_project_dir(capfd):
    with ProjectTemporaryDirectory() as tmp_dir:
        cli(["fal", "run", "--project-dir", tmp_dir, "--profiles-dir", profiles_dir])


def test_version(capfd):
    import pkg_resources

    version = pkg_resources.get_distribution("fal").version
    captured = _run_fal(["--version"], capfd)
    assert f"fal {version}" in captured.out


def test_flow_run_with_project_dir(capfd):
    with ProjectTemporaryDirectory() as tmp_dir:
        captured = _run_fal(
            [
                # fmt: off
                "flow", "run",
                "--project-dir", tmp_dir,
                "--profiles-dir", profiles_dir,
                # fmt: on
            ],
            capfd,
        )

        executing_re = re.compile(
            r": dbt run --threads 1 --project-dir [\w\d\/\-\_]+ --profiles-dir [\w\d\/\-\_]+tests/mock/mockProfile"
        )
        found = executing_re.findall(captured.out)
        # We run each model separately
        assert len(found) == 7


def test_flow_run_with_project_dir_and_select(capfd):
    with ProjectTemporaryDirectory() as tmp_dir:
        captured = _run_fal(
            [
                # fmt: off
                "flow", "run",
                "--project-dir", tmp_dir,
                "--profiles-dir", profiles_dir,
                "--select", "test.py+"
                # fmt: on
            ],
            capfd,
        )

        executing_re = re.compile(
            r": dbt run --threads 1 --project-dir [\w\/\-\_]+ --profiles-dir [\w\/\-\_]+tests/mock/mockProfile \--select|\--models model_with_before_scripts"
        )
        found = executing_re.findall(captured.out)
        assert len(found) == 1
        assert "test.py" in captured.out
        assert (
            "--select model_with_before_scripts"
            or "--models model_with_before_scripts" in captured.out
        )


def test_flow_run_with_defer(capfd):
    with ProjectTemporaryDirectory() as tmp_dir:
        captured = _run_fal(
            [
                # fmt: off
                "flow", "run",
                "--project-dir", tmp_dir,
                "--profiles-dir", profiles_dir,
                "--defer", "--state", (tmp_dir + "/target")
                # fmt: on
            ],
            capfd,
        )

        executing_re = re.compile(
            r": dbt run --threads 1 --project-dir [\w\/\-\_]+ --profiles-dir [\w\/\-\_]+tests/mock/mockProfile --defer --state [\w\/\-\_]+/target"
        )
        found = executing_re.findall(captured.out)
        # We run each model separately
        assert len(found) == 7


def test_flow_run_with_full_refresh(capfd):
    with ProjectTemporaryDirectory() as tmp_dir:
        captured = _run_fal(
            [
                # fmt: off
                "flow", "run",
                "--full-refresh",
                "--project-dir", tmp_dir,
                "--profiles-dir", profiles_dir,
                # fmt: on
            ],
            capfd,
        )

        executing_re = re.compile(
            r": dbt run --threads 1 --project-dir [\w\d\/\-\_]+ --profiles-dir [\w\d\/\-\_]+tests/mock/mockProfile --full-refresh"
        )
        found = executing_re.findall(captured.out)
        # We run each model separately
        assert len(found) == 7


def test_flow_run_with_vars(capfd):
    with ProjectTemporaryDirectory() as tmp_dir:
        captured = _run_fal(
            [
                # fmt: off
                "flow", "run",
                "--project-dir", tmp_dir,
                "--profiles-dir", profiles_dir,
                "--vars", "{some: 'value'}"
                # fmt: on
            ],
            capfd,
        )

        executing_re = re.compile(
            r": dbt run --threads 1 --project-dir [\w\/\-\_]+ --profiles-dir [\w\/\-\_]+tests/mock/mockProfile --vars {some: 'value'}"
        )
        found = executing_re.findall(captured.out)
        # We run each model separately
        assert len(found) == 7


def test_selection(capfd):
    with ProjectTemporaryDirectory() as tmp_dir:
        captured = _run_fal(
            [
                # fmt: off
                "run",
                "--project-dir", tmp_dir,
                "--profiles-dir", profiles_dir,
                "--select", "model_with_scripts", # not included (overwritten)
                "--select", "other_with_scripts",
                # fmt: on
            ],
            capfd,
        )

        assert "model_with_scripts" not in captured.out
        assert "other_with_scripts" in captured.out

        captured = _run_fal(
            [
                # fmt: off
                "flow", "run",
                "--project-dir", tmp_dir,
                "--profiles-dir", profiles_dir,
                "--select", "model_with_scripts",  # not included (overwritten)
                "--select", "model_feature_store", "model_empty_scripts",
                # fmt: on
            ],
            capfd,
        )

        assert "model_with_scripts" not in captured.out
        assert "model_no_fal" not in captured.out
        assert "model_feature_store" in captured.out
        assert "model_empty_scripts" in captured.out

        captured = _run_fal(
            [
                # fmt: off
                "run",
                "--project-dir", tmp_dir,
                "--profiles-dir", profiles_dir,
                "--select", "model_with_scripts",
                # fmt: on
            ],
            capfd,
        )
        assert "model_with_scripts" in captured.out
        assert "model_feature_store" not in captured.out
        assert "model_empty_scripts" not in captured.out
        # It has no keyword in meta
        assert "model_no_fal" not in captured.out


def test_no_run_results(capfd):
    with ProjectTemporaryDirectory() as tmp_dir:
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
    with ProjectTemporaryDirectory() as tmp_dir:

        captured = _run_fal(
            [
                # fmt: off
                "run",
                "--project-dir", tmp_dir,
                "--profiles-dir", profiles_dir,
                "--select", "model_with_scripts",
                "--before",
                # fmt: on
            ],
            capfd,
        )
        assert "model_with_scripts" not in captured.out
        assert "test.py" not in captured.out
        assert "model_with_before_scripts" not in captured.out
        assert "model_feature_store" not in captured.out
        assert "model_empty_scripts" not in captured.out
        assert "model_no_fal" not in captured.out

        captured = _run_fal(
            [
                # fmt: off
                "run",
                "--project-dir", tmp_dir,
                "--profiles-dir", profiles_dir,
                "--before",
                # fmt: on
            ],
            capfd,
        )
        assert "model_with_scripts" not in captured.out
        assert "model_feature_store" not in captured.out
        assert "model_empty_scripts" not in captured.out
        assert "model_no_fal" not in captured.out
        assert "model_with_before_scripts" in captured.out


def test_target(capfd):
    with ProjectTemporaryDirectory() as tmp_dir:

        captured = _run_fal(
            [
                "run",
                "--project-dir",
                tmp_dir,
                "--profiles-dir",
                profiles_dir,
                "--target",
                "false_target",
            ],
            capfd,
        )
        assert (
            "The profile 'fal_test' does not have a target named 'false_target'"
            in captured.out
        )


@pytest.mark.parametrize(
    "broken_schema",
    [
        """
        version: 2
        models:
          - name: model_with_scripts
            meta:
              fal:
                scripts:
                  - path: fal_scripts/test.py
        """,
        """
        version: 2
        models:
          - name: model_with_scripts
            meta:
              fal:
                scripts:
                  after:
                    - path: fal_scripts/test.py
        """,
        """
        version: 2
        models:
          - name: model_with_scripts
            meta:
              fal:
                pre-hook:
                  - xxx: fal_scripts/test.py
        """,
    ],
)
def test_broken_schemas(broken_schema, monkeypatch):
    with ProjectTemporaryDirectory() as tmp_dir:
        monkeypatch.chdir(tmp_dir)
        path = Path(tmp_dir)

        for model in (path / "models").rglob("*.sql"):
            if model.stem != "model_with_scripts":
                model.unlink()

        with open(path / "models" / "schema.yml", "w") as f:
            f.write(inspect.cleandoc(broken_schema))

        with pytest.raises((ValueError, TypeError)):
            cli(
                [
                    "fal",
                    "flow",
                    "run",
                    "--project-dir",
                    tmp_dir,
                    "--profiles-dir",
                    profiles_dir,
                    "--exclude=model_with_scripts",
                ],
            )

@pytest.mark.parametrize(
    "schema",
    [
        """
        version: 2
        models:
          - name: model_with_scripts
            meta:
              fal:
                post-hook:
                  - fal_scripts/test.py
        """,
        """
        version: 2
        models:
          - name: model_with_scripts
            meta:
              fal:
                post-hook:
                  - path: fal_scripts/test.py
        """,
    ],
)
def test_schemas(schema, monkeypatch):
    with ProjectTemporaryDirectory() as tmp_dir:
        monkeypatch.chdir(tmp_dir)
        path = Path(tmp_dir)

        for model in (path / "models").rglob("*.sql"):
            if model.stem != "model_with_scripts":
                model.unlink()

        with open(path / "models" / "schema.yml", "w") as f:
            f.write(inspect.cleandoc(schema))

        cli(
            [
                "fal",
                "flow",
                "run",
                "--project-dir",
                tmp_dir,
                "--profiles-dir",
                profiles_dir,
                "--exclude=model_with_scripts",
            ],
        )

def _run_fal(args, capfd):
    # Given fal arguments, runs fal and returns capfd output
    try:
        cli(["fal"] + args)
    except SystemExit:
        pass
    except Exception as e:
        print(e)
    return capfd.readouterr()
