Feature: `write_to_source` function
  Background: Project Setup
    Given the project 004_script_paths

  Scenario: fal flow run
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following models are calculated:
      | some_model |
    And the following scripts are ran:
    | some_model.before.py | some_model.after.py |

  Scenario: fal run
    When `dbt run --profiles-dir $profilesDir --project-dir $baseDir` is run
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --scripts after.py
      """
    Then the following scripts are ran:
    | some_model.after.py |

  Scenario: fal run with before
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --before
      """
    Then the following scripts are ran:
    | some_model.before.py |
