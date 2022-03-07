Feature: `fal run` works with dbt test command
  Scenario: fal run works after dbt test
    Given `dbt run --profiles-dir .` is run
    And `dbt test --profiles-dir .` is run
    When `fal run --profiles-dir .` is run
    Then scripts are run for agent_wait_time
    And outputs for agent_wait_time contain test results
