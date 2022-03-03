Feature: `run` command
  Scenario: fal run works
    Given dbt run is finished on all models
    When we call `fal run --profiles-dir .`
    Then scripts are run for all models
