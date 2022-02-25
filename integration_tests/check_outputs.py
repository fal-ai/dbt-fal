import sys

models = ["agent_wait_time", "zendesk_ticket_data"]

is_test = len(sys.argv) > 0 and sys.argv[0] == "test"

is_selection = len(sys.argv) > 0 and sys.argv[0] == 'selection'

if is_selection:
    regular = open("regular_output", "r").read()
    script_selected = open("script_selected", "r").read()
    false_script_selected = open("false_script_selected", "r").read()

    assert "fal_scripts/test.py for model zendesk_ticket_data" in regular
    assert "fal_scripts/test.py for model agent_wait_time" in regular
    assert "fal_scripts/john_test.py for model john_table" in regular

    assert "fal_scripts/test.py for model zendesk_ticket_data" in script_selected
    assert "fal_scripts/test.py for model agent_wait_time" in script_selected
    assert "fal_scripts/john_test.py for model john_table" not in script_selected

    assert "fal_scripts/test.py for model zendesk_ticket_data" not in false_script_selected
    assert "fal_scripts/test.py for model agent_wait_time" not in false_script_selected
    assert "fal_scripts/john_test.py for model john_table" not in false_script_selected
    exit()

for model in models:
    try:
        print(f"Checking: {model}")
        if is_test:
            expected = open(f"fal_output/{model}_expected_test", "r").read()
            current = open(f"fal_output/{model}_test", "r").read()
        else:
            expected = open(f"fal_output/{model}_expected", "r").read()
            current = open(f"fal_output/{model}", "r").read()
        assert expected == current
    except AssertionError:
        print(f"Error for {model}:")
        print(f"\n*****\n\nExpected: \n{expected}")
        print(f"\n*****\n\nGot: \n{current}")
        raise Exception("Did not get expected output")

print("Success!")
