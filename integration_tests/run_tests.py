import os

def run_command(command: str, message: str):
    os.system(f"echo '{message}'")
    os.system(f"cd mock && {command}")


run_command(command="dbt seed --profiles-dir .",
            message="*** Seeding database ***")

run_command(command="fal flow run --profiles-dir .",
            message="*** Starting fal flow run ***")

run_command(command="python check_outputs.py",
            message="*** Checking fal output files ***")

run_command(command="dbt run --profiles-dir .",
            message="*** Starting dbt run ***")

run_command(command="fal run --profiles-dir . > regular_output",
            message="*** Starting fal run ***")

run_command(command="python check_outputs.py",
            message="*** Checking fal output files ***")

run_command(command="dbt test --profiles-dir . --model agent_wait_time",
            message="*** Starting dbt test run ***")

run_command(command="fal run --profiles-dir .",
            message="*** Starting fal run ***")

run_command(command="python check_outputs.py test",
            message="*** Checking fal output files ***")

run_command(command="dbt run --profiles-dir .",
            message="*** Starting dbt run ***")

run_command(command="fal run --profiles-dir . --script fal_scripts/test.py > script_selected",
            message="*** Starting fal run ***")

run_command(command="fal run --profiles-dir . --script fal_scripts/notthere.py > false_script_selected",
            message="*** Starting fal run ***")

run_command(command="python check_outputs.py selection",
            message="*** Checking fal output files ***")
