import os

MODELS = ["agent_wait_time", "zendesk_ticket_data"]


def run_command(command: str, message: str):
    os.system(f"echo '{message}'")
    os.system(f"cd mock && {command}")


def check_output(model, is_test=False):
    try:
        os.system(f"echo 'Checking: {model}'")
        if is_test:
            expected = open(f"mock/fal_output/{model}_expected_test", "r").read()
            current = open(f"mock/fal_output/{model}_test", "r").read()
        else:
            expected = open(f"mock/fal_output/{model}_expected", "r").read()
            current = open(f"mock/fal_output/{model}", "r").read()
        assert expected == current
    except AssertionError:
        os.system(f"echo 'Error for {model}:'")
        os.system(f"echo 'Expected: {expected}'")
        os.system(f"echo 'Got: {current}'")
        raise Exception("Did not get expected output")


def check_script_selection():
    os.system("echo 'Checking script selection'")
    regular = open("mock/regular_output", "r").read()
    script_selected = open("mock/script_selected", "r").read()
    false_script_selected = open("mock/false_script_selected", "r").read()

    assert "fal_scripts/test.py for model zendesk_ticket_data" in regular
    assert "fal_scripts/test.py for model agent_wait_time" in regular
    assert "fal_scripts/john_test.py for model john_table" in regular

    assert "fal_scripts/test.py for model zendesk_ticket_data" in script_selected
    assert "fal_scripts/test.py for model agent_wait_time" in script_selected
    assert "fal_scripts/john_test.py for model john_table" not in script_selected

    assert (
        "fal_scripts/test.py for model zendesk_ticket_data" not in false_script_selected
    )
    assert "fal_scripts/test.py for model agent_wait_time" not in false_script_selected
    assert "fal_scripts/john_test.py for model john_table" not in false_script_selected


# Setup dbt database
run_command(command="dbt seed --profiles-dir .", message="*** Seeding database ***")

# Test flow run
run_command(
    command="fal flow run --profiles-dir .", message="*** Starting fal flow run ***"
)

for model in MODELS:
    check_output(model)

run_command(command="dbt run --profiles-dir .", message="*** Starting dbt run ***")

run_command(
    command="fal run --profiles-dir . > regular_output",
    message="*** Starting fal run ***",
)

for model in MODELS:
    check_output(model)

run_command(
    command="dbt test --profiles-dir . --model agent_wait_time",
    message="*** Starting dbt test run ***",
)

run_command(command="fal run --profiles-dir .", message="*** Starting fal run ***")

check_output("agent_wait_time", is_test=True)

run_command(command="dbt run --profiles-dir .", message="*** Starting dbt run ***")

run_command(
    command="fal run --profiles-dir . --script fal_scripts/test.py > script_selected",
    message="*** Starting fal run ***",
)

run_command(
    command="fal run --profiles-dir . --script fal_scripts/notthere.py > false_script_selected",
    message="*** Starting fal run ***",
)

check_script_selection()
