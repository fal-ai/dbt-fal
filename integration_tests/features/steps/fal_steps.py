import os
from behave import *


MODELS = ["agent_wait_time", "zendesk_ticket_data"]


@given("dbt {command} is finished on {model}")
def run_dbt_step(context, command, model):
    if model == "all models":
        os.system(f"cd mock && dbt {command} --profiles-dir .")
    else:
        os.system(f"cd mock && dbt {command} --profiles-dir . --models {model}")


@when("we call `{command}`")
def run_command_step(context, command):
    _run_command(command)


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
