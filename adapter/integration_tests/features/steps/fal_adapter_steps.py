from functools import reduce
import os
from behave import *

import tempfile
import json
from pathlib import Path
from datetime import datetime
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
    os.environ["temp_dir"] = context.temp_dir.name
    os.environ["project_dir"] = context.base_dir


@then("the following models are calculated in order")
def check_model_results(context):
    models = _get_dated_dbt_models(context)
    sorted_models = sorted(models, key=lambda x: x[1])
    sorted_model_names = [model[0] for model in sorted_models]
    expected_model_names = context.table.headings
    assert sorted_model_names == expected_model_names, f"Expected {expected_model_names}, got {sorted_model_names}"


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
    with open(
        os.path.join(context.temp_dir.name, "target", "run_results.json")
    ) as stream:
        return json.load(stream)["results"]


def _set_profiles_dir(context) -> Path:
    # TODO: Ideally this needs to change in just one place
    available_profiles = [
        "postgres",
        "bigquery",
        "redshift",
        "snowflake",
        "duckdb",
        "athena",
    ]
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
