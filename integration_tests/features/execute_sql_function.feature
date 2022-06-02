Feature: `execute_sql` function
  Background: Project Setup
    Given the project 009_execute_sql_function

  Scenario: Use execute_sql function
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-flow
      """
    Then the following models are calculated:
      | other_model | some_model |
    And the following scripts are ran:
      | some_model.query_other_model.py |
    And the script some_model.query_other_model.py output file has the lines:
      | Model dataframe information: | RangeIndex: 1 entries, 0 to 0 |
