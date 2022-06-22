Feature: `dbt test` awareness
  Background: Project Setup
    Given the project 005_functions_and_variables

  @TODO-duckdb
  Scenario: Source tests are present in context
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | some_model.write_to_source_twice.py | other_model.complete_model.py | third_model.complete_model.py |
    And the script some_model.write_to_source_twice.py output file has the lines:
      | source results.some_source has 2 tests, source status is skipped  |
      | source results.other_source has 0 tests, source status is skipped |
      | model some_model has 2 tests, model status is success             |
      | model other_model has 0 tests, model status is success            |
    When the following shell command is invoked:
      """
      dbt test --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | some_model.write_to_source_twice.py |
    And the script some_model.write_to_source_twice.py output file has the lines:
      | source results.some_source has 2 tests, source status is tested   |
      | source results.other_source has 0 tests, source status is skipped |
      | model some_model has 2 tests, model status is tested              |
      | model other_model has 0 tests, model status is skipped            |
