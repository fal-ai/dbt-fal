Feature: `flow run` command
  Background: Project Setup
    Given the project 001_flow_run_with_selectors
    When the data is seeded

  Scenario: fal flow run command with selectors
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select before.py+
    """
    Then the following models are calculated:
    | agent_wait_time | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 |
    And the following scripts are ran:
    | agent_wait_time.before.py | agent_wait_time.after.py |

  Scenario: fal flow run command with selectors
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select before.py
    """
    Then no models are calculated
    And the following scripts are ran:
    | agent_wait_time.before.py |

  Scenario: fal flow run command with selectors
    When the following command is invoked:
    """
    fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select agent_wait_time --threads 1
    """
    Then the following models are calculated:
    | agent_wait_time |

