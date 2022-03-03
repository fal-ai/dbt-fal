Feature: `flow run` command
  Scenario: fal flow run works
    Given dbt run is finished on all models
    When we call `fal flow run --profiles-dir .`
    Then scripts are run for all models
