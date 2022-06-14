from functools import reduce
import os
from typing import List
from behave import *
import glob
from fal.cli import cli
import tempfile
import json
import unittest
from os.path import exists
from pathlib import Path
from datetime import datetime, timezone
import re


# The main distinction we can use on an artifact file to determine
# whether it was created by a Python script or a Python model is the number
# of suffixes it has. Models use <model_name>.txt and scripts use
# <model_name>.<script_name>.txt

FAL_MODEL = 1
FAL_SCRIPT = 2


@when("the following shell command is invoked")
def run_command_step(context):
    profiles_dir = _set_profiles_dir(context)
    command = (
        context.text.replace("$baseDir", context.base_dir)
        .replace("$profilesDir", str(profiles_dir))
        .replace("$tempDir", str(context.temp_dir.name))
    )
    os.system(command)


@given("the project {project}")
def set_project_folder(context, project):
    context.base_dir = reduce(os.path.join, [os.getcwd(), "projects", project])
    context.temp_dir = tempfile.TemporaryDirectory()
    os.environ["temp_dir"] = context.temp_dir.name
    os.environ["project_dir"] = context.base_dir


@when("the data is seeded")
def seed_data(context):
    base_path = Path(context.base_dir)
    profiles_dir = _set_profiles_dir(context)
    os.system(
        f"dbt seed --profiles-dir {profiles_dir} --project-dir {base_path} --full-refresh"
    )


@when("the data is seeded to {target} target in profile directory {profiles_dir}")
def seed_data_custom_target(context, target, profiles_dir):
    base_path = Path(context.base_dir)
    os.system(
        f"dbt seed --profiles-dir {profiles_dir} --project-dir {base_path} --full-refresh --target {target}"
    )


@when("state is stored in {folder_name}")
def persist_state(context, folder_name):
    target_path = reduce(os.path.join, [context.temp_dir.name, "target"])
    os.system(f"mv {target_path} {context.temp_dir.name}/{folder_name}")


@when("the file {file} is created with the content")
def add_model(context, file):
    file = file.replace("$baseDir", context.base_dir)
    context.added_during_tests = (
        context.added_during_tests.append(file)
        if hasattr(context, "added_during_tests")
        else [file]
    )
    parent = os.path.dirname(file)
    if not exists(parent):
        os.mkdir(parent)
    with open(file, "w") as f:
        f.write(context.text)
        f.close()


@when("the following command is invoked")
def invoke_command(context):
    _clear_all_artifacts(context.temp_dir.name)
    profiles_dir = _set_profiles_dir(context)
    args: str = context.text.replace("$baseDir", context.base_dir)
    args = args.replace("$profilesDir", str(profiles_dir))
    args = args.replace("$tempDir", context.temp_dir.name)

    import shlex

    context.exc = None
    try:
        cli(shlex.split(args))
    except Exception:
        import sys

        context.exc = sys.exc_info()


@then("it throws an exception {etype} with message '{msg}'")
def invoke_command_error(context, etype: str, msg: str):
    _etype, exception, _tb = context.exc
    assert isinstance(
        exception, eval(etype)
    ), f"Invalid exception - expected {type}, got {type(exception)}"
    assert msg in str(exception), "Invalid message - expected " + msg

    # Clear the exception
    context.exc = None


@then("the following command will fail")
def invoke_failing_fal_flow(context):
    profiles_dir = _set_profiles_dir(context)
    args = context.text.replace("$baseDir", context.base_dir)
    args = args.replace("$profilesDir", str(profiles_dir))
    args = args.replace("$tempDir", str(context.temp_dir.name))
    try:
        cli(args.split(" "))
        assert False, "Command should have failed."
    except Exception as e:
        print(e)


@then("the following scripts are ran")
def check_script_files_exist(context):
    python_scripts = _get_fal_scripts(context)
    expected_scripts = list(map(_script_filename, context.table.headings))
    unittest.TestCase().assertCountEqual(python_scripts, expected_scripts)


@then("the following scripts are not ran")
def check_script_files_dont_exist(context):
    python_scripts = set(_get_fal_scripts(context))
    expected_scripts = set(map(_script_filename, context.table.headings))

    unexpected_runs = expected_scripts & python_scripts
    if unexpected_runs:
        to_report = ", ".join(unexpected_runs)
        assert False, f"Script files {to_report} should NOT BE present"


def _clear_all_artifacts(dir_name):
    """Clear all artifacts that are left behind by Python scripts and models."""
    directory = Path(dir_name)
    for artifact in directory.glob("*.txt"):
        artifact.unlink()


@then("the script {script} output file has the lines")
def check_file_has_lines(context, script):
    filename = _script_filename(script)
    with open(_temp_dir_path(context, filename)) as handle:
        handle_lines = [line.strip().lower() for line in handle]
        expected_lines = [line.lower() for line in context.table.headings]
        for line in expected_lines:
            assert line in handle_lines, f"{line} not in {handle_lines}"


@then("no models are calculated")
def no_models_are_run(context):
    fal_results = _get_fal_results_file_name(context)
    fal_results_paths = list(
        map(lambda file: os.path.join(context.temp_dir.name, file), fal_results)
    )
    for fal_result_path in fal_results_paths:
        if exists(fal_result_path):
            data = json.load(open(fal_result_path))
            assert (
                len(data["results"]) == 0
            ), f"results length is {len(data['results'])}"
        else:
            assert True


@then("no scripts are run")
def no_scripts_are_run(context):
    results = glob.glob(f"{context.temp_dir.name}/*.txt")

    assert len(results) == 0


@then("the following models are calculated")
def check_model_results(context):
    models = _get_all_models(context)
    expected_models = list(map(_script_filename, context.table.headings))
    unittest.TestCase().assertCountEqual(models, expected_models)


@then("the following nodes are calculated in the following order")
def check_model_results(context):
    models = _get_dated_dbt_models(context)
    scripts = _get_dated_fal_artifacts(context, FAL_MODEL, FAL_SCRIPT)

    ordered_nodes = _unpack_dated_result(models + scripts)
    expected_nodes = list(map(_script_filename, context.table.headings))
    unittest.TestCase().assertEqual(ordered_nodes, expected_nodes)


def _script_filename(script: str):
    return script.replace(".ipynb", ".txt").replace(".py", ".txt")


def _temp_dir_path(context, file):
    return os.path.join(context.temp_dir.name, file)


def _get_all_models(context) -> List[str]:
    """Retrieve all models (both DBT and Python)."""
    dbt_models = _unpack_dated_result(_get_dated_dbt_models(context))
    python_models = _unpack_dated_result(_get_dated_fal_artifacts(context, FAL_MODEL))

    models = dbt_models + python_models
    return models


def _get_fal_scripts(context) -> List[str]:
    return _unpack_dated_result(_get_dated_fal_artifacts(context, FAL_SCRIPT))


def _unpack_dated_result(dated_result) -> List[str]:
    if not dated_result:
        return []

    result, _ = zip(*sorted(dated_result, key=lambda node: node[1]))
    return list(result)


def _get_dated_dbt_models(context):
    return [
        (
            result["unique_id"].split(".")[-1],
            datetime.fromisoformat(
                result["timing"][-1]["completed_at"].replace("Z", "+00:00")
            ),
        )
        for result in _load_dbt_result_file(context)
    ]


def _get_dated_fal_artifacts(context, *kinds):
    assert kinds, "Specify at least one artifact kind."

    directory = Path(context.temp_dir.name)
    return [
        # DBT run result files use UTC as the timezone for the timestamps, so
        # we need to be careful on using the same method for the local files as well.
        (
            artifact.name,
            datetime.fromtimestamp(artifact.stat().st_mtime, tz=timezone.utc),
        )
        for artifact in directory.glob("*.txt")
        if len(artifact.suffixes) in kinds
    ]


def _load_dbt_result_file(context):
    with open(
        os.path.join(context.temp_dir.name, "target", "run_results.json")
    ) as stream:
        return json.load(stream)["results"]


def _get_fal_results_file_name(context):
    target_path = os.path.join(context.temp_dir.name, "target")
    pattern = re.compile("fal_results_*.\\.json")
    target_files = list(os.walk(target_path))[0][2]
    return list(filter(lambda file: pattern.match(file), target_files))


def _set_profiles_dir(context) -> Path:
    # TODO: Ideally this needs to change in just one place
    available_profiles = ["postgres", "bigquery", "redshift", "snowflake", "duckdb"]
    if "profile" in context.config.userdata:
        profile = context.config.userdata["profile"]
        if profile not in available_profiles:
            raise Exception(f"Profile {profile} is not supported")
        raw_path = reduce(os.path.join, [os.getcwd(), "profiles", profile])
        path = Path(raw_path).absolute()
    elif "profiles_dir" in context:
        path = Path(context.profiles_dir).absolute()
    else:
        # Use postgres profile
        path = Path(context.base_dir).parent.absolute()

    os.environ["profiles_dir"] = str(path)
    return path
