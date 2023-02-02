Feature: Python models with defined envs
  Background: Project Setup
    Given the project env_project

  Scenario: Run a project with a Python model that is calculated in a virtual environment
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --select +model_c+
      """
    Then the following models are calculated in order:
      | model_a | model_c | model_d |

  Scenario: Running a similar model without environment fails
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --select +model_b+
      """
    Then model model_b fails with message "No module named 'pyjokes'"
