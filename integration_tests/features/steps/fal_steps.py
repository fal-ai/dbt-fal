import os
from behave import *

os.system("mkdir mock/temp")

MODELS = ["agent_wait_time", "zendesk_ticket_data"]


@given("dbt run is finished on {model}")
def run_dbt_step(context, model):
    _run_command("dbt seed --profiles-dir .")
    if model == "all models":
        _run_command("dbt run --profiles-dir .")


@when("we call `{command}`")
def run_command_step(context, command):
    print(command)
    # _run_command(command)
    _run_command("fal flow run --profiles-dir .")


@then("scripts are run for {model}")
def check_results_step(context, model):
    if model == "all models":
        for model in MODELS:
            _check_output(model)
    _clean_up()


def _run_command(command: str):
    os.system(f"cd mock && {command}")


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


def _clean_up():
    os.system("rm -rf mock/temp/*")
