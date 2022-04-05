Feature: `flow run` command

  Scenario: fal flow run command with selectors
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select before.py+
    """
    Then the following models are calculated:
    | agent_wait_time | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 |
    And the following scripts are ran:
    | agent_wait_time.before.py | agent_wait_time.after.py |

  Scenario: fal flow run command with selectors
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select before.py
    """
    Then no models are calculated
    And the following scripts are ran:
    | agent_wait_time.before.py |

  Scenario: fal flow run command with selectors
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select agent_wait_time --threads 1
    """
    Then the following models are calculated:
    | agent_wait_time |

  Scenario: fal flow run command with state selector without state
    Given the project 001_flow_run_with_selectors
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --threads 1
    """
    And model named new_model is added
    Then the following command will fail:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select state:new --threads 1
    """
    And no models are calculated
    And model named new_model is removed

  Scenario: fal flow run command with state selector and with state
    Given the project 001_flow_run_with_selectors
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --threads 1
    """
    And state is stored in old_state
    And model named new_model is added
    And the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select state:new --state $tempDir/old_state --threads 1
    """
    Then the following models are calculated:
    | new_model |
    And model named new_model is removed

  Scenario: fal flow run with an error in before
    Given the project 003_scripts_with_errors
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select before.py
    """
    Then it throws an RuntimeError exception with message 'Error in scripts'


  Scenario: fal flow run with an error in after
    Given the project 003_scripts_with_errors
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select after.py
    """
    Then it throws an RuntimeError exception with message 'Error in scripts'

  Scenario: fal flow run with an error in dbt run
    Given the project 003_scripts_with_errors
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select some_model
    """
    Then it throws an RuntimeError exception with message 'Error running dbt run'

  Scenario: fal flow run command with selectors with complex selectors
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select tag:daily
    """
    Then the following models are calculated:
    | agent_wait_time |

  Scenario: fal flow run command with complex selectors and children
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select tag:daily+
    """
    Then the following models are calculated:
    | agent_wait_time | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 |
    And the following scripts are ran:
    | agent_wait_time.after.py |
