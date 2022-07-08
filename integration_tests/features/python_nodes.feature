Feature: Python nodes
  Background: Project Setup
    Given the project 008_pure_python_models

  Scenario: Run a project with Python nodes
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --exclude broken_model
      """
    Then the following models are calculated:
      | model_a | model_b | model_c.py | model_d | model_e.ipynb |
    Then the following scripts are ran:
      | model_a.after.py | model_c.after.py | model_e.after.py | model_c.post_hook.py |

  Scenario: Run a project with Python nodes only selecting the Python model
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select model_c
      """
    Then the following models are calculated:
      | model_c.py |
    And the following scripts are not ran:
      | model_c.after.py |
    And the following scripts are ran:
      | model_c.post_hook.py |
    And the script model_c.post_hook.py output file has the lines:
      | Status: success |

  Scenario: Python model post hooks run even when model script fails
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select broken_model
      """
    Then the following scripts are ran:
      | broken_model.post_hook.py |
    And the script broken_model.post_hook.py output file has the lines:
      | Status: error |
    And it throws an exception RuntimeError with message 'Error in scripts (model: models/staging/broken_model.py)'

  Scenario: Run a Python node without write to model should error
    Given the project 003_scripts_with_errors
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select no_write_model
      """
    Then it throws an exception AssertionError with message 'There must be at least one write_to_model call in the Python Model'
