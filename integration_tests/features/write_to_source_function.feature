Feature: `write_to_source` function
  Background: Project Setup
    Given the project 005_functions_and_variables

  Scenario:
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following models are calculated:
      | some_model |
    And the following scripts are ran:
      | some_model.after.py |
    And the model some_model.after.py output file has the lines:
      | results.some_source size: 1 | results.some_source size: 2 |
