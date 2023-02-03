Feature: Context object information
  Background: Project Setup
    Given the project 005_functions_and_variables
    # To make sure all data is there for dbt stage
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """

  Scenario: Get target info in post hook
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select some_model
      """
    Then the following models are calculated:
      | some_model |
    Then the following scripts are ran:
      | some_model.lists.py | some_model.context.py | some_model.execute_sql.py |
    And the script some_model.context.py output file has the lines:
      | target profile: fal_test |

  Scenario: Get rows affected in post hook for fal flow run
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select some_model
      """
    Then the following models are calculated:
      | some_model |
    Then the following scripts are ran:
      | some_model.lists.py | some_model.context.py | some_model.execute_sql.py |
    And the script some_model.context.py output file has the lines:
      | adapter response: rows affected 1 |

  Scenario: Get rows affected in post hook for fal run
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --select some_model --scripts context.py
      """
    Then the following scripts are ran:
      | some_model.lists.py | some_model.context.py | some_model.execute_sql.py |
    And the script some_model.context.py output file has the lines:
      | adapter response: rows affected 1 |

