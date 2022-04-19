Feature: `write_to_model` function
  Background: Project Setup
    Given the project 005_functions_and_variables

  Scenario: Use write_to_model function with mode overwrite
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-flow
      """
    Then the following models are calculated:
      | other_model | some_model |
    And the following scripts are ran:
      | some_model.write_to_source_twice.py | other_model.complete_other_model.py |
    And the script other_model.complete_other_model.py output file has the lines:
      | my_float None | my_float 2.3 | size 1 |
    And the script some_model.write_to_source_twice.py output file has the lines:
      | my_float 2.3 |
