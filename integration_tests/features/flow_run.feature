Feature: `flow run` command
  Scenario: fal flow run works
    Given `dbt run --profiles-dir .` is run
    When `fal flow run --profiles-dir .` is run
    Then scripts are run for all models
    And outputs for all models contain run results
