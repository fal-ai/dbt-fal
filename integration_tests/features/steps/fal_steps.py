from functools import reduce
import os
from behave import *
from fal.cli import cli
import tempfile
import json
import unittest
from os.path import exists
import glob
from pathlib import Path
import shutil

MODELS = ["agent_wait_time", "zendesk_ticket_data"]


@given("`{command}` is run")
def run_command_step(context, command):
    _run_command(command)


@when("`{command}` is run")
def run_command_step2(context, command):
    _run_command(command)


@given("the project {project}")
def set_project_folder(context, project):
    context.base_dir = reduce(os.path.join, [os.getcwd(), "projects", project])
    context.temp_dir = tempfile.TemporaryDirectory()
    os.environ["temp_dir"] = context.temp_dir.name


@when("the data is seeded")
def seed_data(context):
    base_path = Path(context.base_dir)
    profiles_dir = str(base_path.parent.absolute())
    os.system(f"dbt seed --profiles-dir {profiles_dir} --project-dir {base_path}")


@when("the following command is invoked")
def invoke_fal_flow(context):
    profiles_dir = Path(context.base_dir).parent.absolute()
    args = context.text.replace("$baseDir", context.base_dir)
    args = args.replace("$profilesDir", str(profiles_dir))
    cli(args.split(" "))


@then("the following scripts are ran")
def check_script_results(context):
    expected_scripts = context.table.headings
    for script in expected_scripts:
        _assertScriptFileExists(context, script)


@then("no models are calculated")
def no_models_are_run(context):
    run_results_path = reduce(
        os.path.join, [context.base_dir, "target", "run_results.json"]
    )
    if exists(run_results_path):
        data = json.load(open(run_results_path))
        assert len(data["results"]) == 0
    else:
        assert True


@then("no scripts are run")
def no_scripts_are_run(context):
    assert len(os.listdir(context.temp_dir.name)) == 0


@then("the following models are calculated")
def check_model_results(context):
    run_results = open(
        reduce(os.path.join, [context.temp_dir.name, "target", "run_results.json"])
    )
    data = json.load(run_results)
    calculated_results = map(
        lambda result: result["unique_id"].split(".")[2], data["results"]
    )
    unittest.TestCase().assertCountEqual(calculated_results, context.table.headings)


@then("scripts are run for {model}")
def check_run_step(context, model):
    output = open("mock/temp/output", "r").read()

    if model == "all models":
        for m in MODELS:
            assert m in output
    else:
        assert model in output


@then("{model} scripts are skipped")
def check_no_run_step(context, model):
    output = open("mock/temp/output", "r").read()
    if model == "all model":
        for m in MODELS:
            assert m not in output
    else:
        assert model not in output


@then("outputs for {model} contain {run_type} results")
def check_outputs(context, model, run_type):
    test_results = run_type == "test"
    if model == "all models":
        for m in MODELS:
            _check_output(m, test_results)
    else:
        _check_output(model, test_results)


def _assertScriptFileExists(context, script):
    script_file = os.path.join(context.temp_dir.name, script)
    assert exists(script_file)


def _run_command(command: str):
    os.system(f"cd mock && {command} > temp/output")


def _check_output(model, is_test=False):
    try:
        print(f"Checking: {model}", flush=True)
        if is_test:
            expected = open(f"mock/fal_output/{model}_expected_test", "r").read()
            current = open(f"mock/temp/{model}_test", "r").read()
        else:
            expected = open(f"mock/fal_output/{model}_expected", "r").read()
            current = open(f"mock/temp/{model}", "r").read()
        assert expected == current
    except AssertionError:
        print(f"Error for {model}:", flush=True)
        print(f"Expected: {expected}", flush=True)
        print(f"Got: {current}", flush=True)
        raise Exception("Did not get expected output")
