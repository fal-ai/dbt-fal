Feature: `flow run` command
  Scenario: fal flow run works
    Given `dbt run --profiles-dir .` is run
    When `fal flow run --profiles-dir .` is run
    Then scripts are run for all models
    And outputs for all models contain run results


  Scenario: fal flow run command with selectors
    Given the project 001_flow_run_with_selectors
    When fal flow is invoked with following arguements:
    """
    --profiles-dir $baseDir --project-dir $baseDir --select before.py+
    """
    Then the following models are calculated:
    | agent_wait_time |
    And the following scripts are ran:
    | agent_wait_time.before.py | agent_wait_time.after.py |
    And there are no errors

  Scenario: fal flow run command with selectors
    Given the project 001_flow_run_with_selectors
    When fal flow is invoked with following arguements:
    """
    --profiles-dir $baseDir --project-dir $baseDir --select before.py
    """
    Then no models are calculated
    And the following scripts are ran:
    | agent_wait_time.before.py |
    And there are no errors

  Scenario: fal flow run command with selectors
    Given the project 001_flow_run_with_selectors
    When fal flow is invoked with following arguements:
    """
    --profiles-dir $baseDir --project-dir $baseDir --select agent_wait_time
    """
    Then the following models are calculated:
    | agent_wait_time |
    And there are no errors

