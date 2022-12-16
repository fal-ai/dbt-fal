Feature: Python models
  Background: Project Setup
    Given the project simple_project

  Scenario: Run a project with a SQL model
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following models are calculated in order:
      | model_a | model_b | model_c | model_d |

  Scenario: Get datawarehouse for target.type, env.type and adapter.type()
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --select model_a
      """
    Then the compiled SQL model model_a has the string "-- adapter.type() = $profile"
    Then the compiled SQL model model_a has the string "-- target.type = $profile"
    Then the compiled SQL model model_a has the string "-- env.type = $profile"
