from functools import reduce
import os
from behave import *

import tempfile
import json
import yaml
from pathlib import Path
from datetime import datetime
import re


def target_path(context):
    return str(Path(context.temp_dir.name) / "target")


@when("the following shell command is invoked")
def run_command_step(context):
    context.exc = None

    profiles_dir = _get_profiles_dir(context)

    command = _replace_vars(context, context.text)
    try:
        os.system(command)
    except:
        import sys

        context.exc = sys.exc_info()


@given("the project {project}")
def set_project_folder(context, project: str):
    project_path = Path.cwd() / "projects" / project
    if not project_path.exists() or not project_path.is_dir():
        extra = ""
        try:
            # Try to find the correct option
            match = re.match("^(\\d+)_", project)

            if match:
                project_number = match.group(1)
                projects_dir = Path(__file__).parent.parent.parent / "projects"
                found = [r.name for r in projects_dir.glob(project_number + "_*")]
                if found:
                    extra = "Is it " + " or ".join(found) + " ?"
        finally:
            raise ValueError(f"Project {project} not found. {extra}")

    context.base_dir = str(project_path)
    context.temp_dir = tempfile.TemporaryDirectory()
    context.project_name = _load_dbt_project_file(context)["name"]
    os.environ["DBT_TARGET_PATH"] = target_path(context)


@then("the following models are calculated in order")
def check_model_results(context):
    models = _get_dated_dbt_models(context)
    sorted_models = sorted(models, key=lambda x: x[1])
    sorted_model_names = [model[0] for model in sorted_models]
    expected_model_names = context.table.headings
    assert (
        sorted_model_names == expected_model_names
    ), f"Expected {expected_model_names}, got {sorted_model_names}"


@then('model {model_name} fails with message "{msg}"')
def invoke_command_error(context, model_name: str, msg: str):
    results = _load_dbt_result_file(context)
    model_result = [i for i in results if model_name in i["unique_id"]][0]
    print(model_result)
    assert model_result["status"] == "error"
    assert model_result["message"] == msg


@then('the compiled {model_type} model {model_name} has the string "{msg}"')
def check_compiled_model(context, model_type: str, model_name: str, msg: str):
    model_type = model_type.lower()
    msg = _replace_vars(context, msg)
    assert model_type in ["sql", "python", "py"], "model type should be SQL or Python"
    if model_type == "python":
        model_type = "py"
    compiled = _load_target_run_model(context, model_name, model_type)
    assert msg in compiled, f'Expected "{msg}" not present in compiled model {compiled}'


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


def _load_dbt_result_file(context):
    with open(os.path.join(target_path(context), "run_results.json")) as stream:
        return json.load(stream)["results"]


def _load_dbt_project_file(context):
    with open(os.path.join(context.base_dir, "dbt_project.yml")) as stream:
        return yaml.full_load(stream)


def _load_target_run_model(context, model_name: str, file_ext: str):
    # TODO: we should use fal to find these files from fal reading the dbt_project.yml and making it easily available
    models_dir: Path = (
        Path(target_path(context)) / "run" / context.project_name / "models"
    )

    found_model_files = list(models_dir.rglob(f"{model_name}.{file_ext}"))

    assert len(found_model_files) == 1, "Model must be unique in models directory"

    return found_model_files[0].read_text()


def _replace_vars(context, msg):
    return (
        msg.replace("$baseDir", context.base_dir)
        .replace("$profilesDir", _get_profiles_dir(context))
        .replace("$profile", _get_profile(context))
    )


def _get_profile(context) -> str:
    if "profile" in context:
        return context.profile

    # TODO: Ideally this needs to change in just one place
    available_profiles = [
        "postgres",
        "bigquery",
        "redshift",
        "snowflake",
        "duckdb",
        "athena",
        "trino",
        "sqlserver",
    ]
    profile = context.config.userdata.get("profile", "postgres")
    if profile not in available_profiles:
        raise Exception(f"Profile {profile} is not supported")

    context.profile = profile
    return context.profile


def _get_profiles_dir(context):
    if "profiles_dir" in context:
        return context.profiles_dir

    profile = _get_profile(context)
    raw_path = reduce(os.path.join, [os.getcwd(), "profiles", profile])
    path = Path(raw_path).absolute()

    context.profiles_dir = str(path)
    return context.profiles_dir
