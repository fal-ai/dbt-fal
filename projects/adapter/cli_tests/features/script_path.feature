Feature: defining script path
  Background: Project Setup
    Given the project 006_script_paths
    When the data is seeded

  Scenario: fal flow run
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following models are calculated:
      | some_model |
    And the following scripts are ran:
      | some_model.before.py | some_model.after.py |

  Scenario: fal flow run with cli var
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --vars "{'fal-scripts-path': 'scripts2'}"
      """
    Then the following models are calculated:
      | some_model |
    And the following scripts are ran:
      | some_model.before2.py | some_model.after2.py |


  Scenario: fal run with before
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --before
      """
    Then the following scripts are ran:
      | some_model.before.py |

  Scenario: fal run
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | some_model.after.py |
