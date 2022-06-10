Feature: Python nodes
  Background: Project Setup
    Given the project 008_pure_python_models

  Scenario: Run a project with Python nodes
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models
      """
    Then the following models are calculated:
      | model_a | model_b | model_c.py | model_d | model_e.ipynb |
    Then the following scripts are ran:
      | model_c.after.py | model_e.after.py |

  Scenario: Run a project with Python nodes only selecting the Python model
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select model_c
      """
    Then the following models are calculated:
      | model_c.py |
    Then the following scripts are not ran:
      | model_c.after.py |

  Scenario: Run a Python node without write to model should error
    Given the project 003_scripts_with_errors
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select no_write_model
      """
    Then it throws an exception AssertionError with message 'There must be a single write_to_model call and it must be a top-level expression of the Python Model script'
