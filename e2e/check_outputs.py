import sys
models = ["agent_wait_time", "zendesk_ticket_data"]

is_test = len(sys.argv) > 0 and sys.argv[0] == 'test'

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
        print(f"Expected: {expected}")
        print(f"Got: {current}")
        raise Exception("Did not get expected output")

print("Success!")
