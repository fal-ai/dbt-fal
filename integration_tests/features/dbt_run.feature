Feature: `fal run` works with dbt test command
  Scenario: fal run works after dbt test
    Given dbt test is finished on all models
    When we call `fal run --profiles-dir .`
    Then scripts are run for agent_wait_time
    And outputs for agent_wait_time contain test results
