Feature: Python nodes
  Background: Project Setup
    Given the project 008_pure_python_models

  Scenario: Run a project with Python nodes
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models
      """
    Then the following models are calculated:
      | model_a | model_b | model_c | model_d |
    And the following scripts are ran:
      | model_c.after.py |

  Scenario: Run a project with Python nodes only selecting the Python model
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select model_c
      """
    Then the following models are calculated:
      | model_c |
    And the following scripts are not ran:
      | model_c.after.py |

  Scenario: Modifying a generated SQL throws an error
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select model_c
      """
    Then the following models are calculated:
      | model_c |
    And the following scripts are not ran:
      | model_c.after.py |
