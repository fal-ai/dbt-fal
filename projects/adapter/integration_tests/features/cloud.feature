@cloud
Feature: isolate cloud
  Background: Project Setup
    Given the project env_project

  Scenario: Run a Python model with Isolate Cloud
    When the following shell command is invoked:
      """
      dbt --debug run --profiles-dir $profilesDir --project-dir $baseDir -t prod --select +model_c+
      """
    Then there should be no errors
    Then the following models are calculated in order:
      | model_a | model_c | model_d |

  Scenario: Run a Python model with Isolate Cloud and conda
    When the following shell command is invoked:
      """
      dbt --debug run --profiles-dir $profilesDir --project-dir $baseDir -t prod --select +model_e
      """
    Then there should be no errors
    Then the following models are calculated in order:
      | model_a | model_e |
