Feature: `write_to_source` function
  Background: Project Setup
    Given the project 005_functions_and_variables

  @TODO-duckdb
  Scenario: Use write_to_source function with mode append and overwrite
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --exclude model_with_date model_with_array
      """
    Then the following models are calculated:
      | other_model | some_model | third_model |
    And the following scripts are ran:
      | some_model.write_to_source_twice.py | some_model.context.py | some_model.lists.py | some_model.execute_sql.py | other_model.complete_model.py | third_model.complete_model.py |
    And the script some_model.write_to_source_twice.py output file has the lines:
      | source size 1 | source size 2 |
