Feature: Python models
  Background: Project Setup
    Given the project simple_project

  Scenario: Run a project with a Python model
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following models are calculated in order:
      | model_a | model_b | model_c | model_d |
