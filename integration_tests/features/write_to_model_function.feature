Feature: `write_to_model` function
  Background: Project Setup
    Given the project 005_functions_and_variables

  @TODO-duckdb
  Scenario: Use write_to_model and write_to_source_twice function with mode overwrite
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-flow
      """
    Then the following models are calculated:
      | other_model | some_model | third_model |
    And the following scripts are ran:
      | some_model.write_to_source_twice.py | other_model.complete_model.py | third_model.complete_model.py |
    And the script other_model.complete_model.py output file has the lines:
      | my_int 0.0 | my_int 3.0 | size 1 |
    And the script third_model.complete_model.py output file has the lines:
      | my_int 0.0 | my_int 3.0 | size 1 |
    And the script some_model.write_to_source_twice.py output file has the lines:
      | my_float 1.2 |

  Scenario: Use write_to_model function with mode overwrite
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select other_model+ --exclude some_model+ --experimental-flow
      """
    Then the following models are calculated:
      | other_model |
    And the following scripts are ran:
      | other_model.complete_model.py |
    And the script other_model.complete_model.py output file has the lines:
      | my_int 0.0 | my_int 3.0 | size 1 |

  @TODO-postgres
  @TODO-snowflake
  @TODO-duckdb
  @TODO-redshift
  Scenario: Write a datetime to the datawarehouse
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select model_with_date
      """
    Then the following models are calculated:
      | model_with_date.py |
    And the script model_with_date.py output file has the lines:
      | my_date: 2022-01-01 00:00:00+00:00 |

  @TODO-postgres
  @TODO-snowflake
  @TODO-duckdb
  @TODO-redshift
  Scenario: Write a string and int array to the datawarehouse
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select model_with_array
      """
    Then the following models are calculated:
      | model_with_array.py |
    And the script model_with_array.py output file has the lines:
      | my_array: ['some', 'other'] | other_array: [1, 2, 3] |
