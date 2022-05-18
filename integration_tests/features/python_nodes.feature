Feature: Python nodes
  Background: Project Setup
    Given the project 008_pure_python_models

  Scenario: Run a project with Python nodes
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models
      """
    # TODO: Python generated models are not included in the run_results.json because they are ephemeral
    # Then the following models are calculated:
    #   | model_a | model_b | model_c | model_d |
    Then the following models are calculated:
      | model_a | model_b | model_d |
    Then the following scripts are ran:
      | model_c.after.py |

  Scenario: Run a project with Python nodes only selecting the Python model
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select model_c
      """
    # TODO: Python generated models are not included in the run_results.json because they are ephemeral
    # Then the following models are calculated:
    #   | model_c |
    Then the following scripts are not ran:
      | model_c.after.py |

