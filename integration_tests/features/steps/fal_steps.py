from functools import reduce
import os
from typing import Dict
from behave import *
from fal.cli import cli
import tempfile
import json
import unittest
from os.path import exists
from pathlib import Path
import re


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


@then("it throws an {etype} exception with message '{msg}'")
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
    python_scripts = _get_all_python_scripts(context.temp_dir.name)
    expected_scripts = list(map(_script_filename, context.table.headings))
    unittest.TestCase().assertCountEqual(python_scripts, expected_scripts)


@then("the following scripts are not ran")
def check_script_files_dont_exist(context):
    python_scripts = set(_get_all_python_scripts(context.temp_dir.name))
    expected_scripts = set(map(_script_filename, context.table.headings))

    unexpected_runs = expected_scripts & python_scripts
    if unexpected_runs:
        to_report = ", ".join(unexpected_runs)
        assert False, f"Script files {to_report} should NOT BE present"


def _get_all_python_scripts(dir_name):
    directory = Path(dir_name)
    return [
        model.name
        for model in directory.glob("*.txt")
        # Pure Python models use <model_name>.txt and scripts use <model_name>.<script>.txt,
        # so this check ensures that we are only capturing the scripts (and not models).
        if len(model.suffixes) == 2
    ]


def _clear_all_artifacts(dir_name):
    """Clear all artifacts that are left behind by Python scripts and models."""
    directory = Path(dir_name)
    for artifact in directory.glob("*.txt"):
        artifact.unlink()


@then("the script {script} output file has the lines")
def check_file_has_lines(context, script):
    filename = _script_filename(script)
    with open(_temp_dir_path(context, filename)) as handle:
        handle_lines = [line.strip() for line in handle]
        expected_lines = context.table.headings
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
    assert len(os.listdir(context.temp_dir.name)) == 0


@then("the following models are calculated")
def check_model_results(context):
    models = _get_models_from_result(
        context.temp_dir.name,
        reduce(os.path.join, [context.temp_dir.name, "target", "run_results.json"]),
    )
    unittest.TestCase().assertCountEqual(_flatten_list(models), context.table.headings)


def _script_filename(script: str):
    return script.replace(".py", ".txt")


def _temp_dir_path(context, file):
    return os.path.join(context.temp_dir.name, file)


def _get_models_from_result(dir_name, file_name):
    return list(
        map(
            lambda result: result["unique_id"].split(".")[2],
            _load_result(dir_name, file_name),
        )
    )


def _load_result(dir_name, file_name):
    return json.load(
        open(
            reduce(
                os.path.join,
                [dir_name, "target", file_name],
            )
        )
    )["results"]


def _get_fal_results_file_name(context):
    target_path = os.path.join(context.temp_dir.name, "target")
    pattern = re.compile("fal_results_*.\\.json")
    target_files = list(os.walk(target_path))[0][2]
    return list(filter(lambda file: pattern.match(file), target_files))


def _flatten_list(target_list):
    flat_list = []
    for element in target_list:
        if type(element) is list:
            for item in element:
                flat_list.append(item)
        else:
            flat_list.append(element)
    return flat_list


def _set_profiles_dir(context) -> Path:
    #TODO: Ideally this needs to change in just one place
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
