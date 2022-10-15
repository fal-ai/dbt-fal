Feature: Access to meta object from schema
  Background: Project Setup
    Given the project 005_functions_and_variables

  Scenario: Use meta object from models
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select some_model
      """
    Then the following models are calculated:
      | some_model |
    And the following scripts are ran:
      | some_model.lists.py | some_model.context.py | some_model.execute_sql.py |
    And the script some_model.lists.py output file has the lines:
      # only check for 1 of the lines
      | model: some_model property: 1 |

  Scenario: Use meta object from sources
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select some_model
      """
    Then the following models are calculated:
      | some_model |
    And the following scripts are ran:
      | some_model.lists.py | some_model.context.py | some_model.execute_sql.py |
    And the script some_model.lists.py output file has the lines:
      | source: results other_source property: 4 | source: results some_source property: None |
