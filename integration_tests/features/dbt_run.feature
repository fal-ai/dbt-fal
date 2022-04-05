Feature: `fal run` works with dbt test command
  Scenario: fal run works after dbt test
    Given the project 000_fal_run
    When the data is seeded
    And `dbt run --profiles-dir $profilesDir --project-dir $baseDir` is run
    And `dbt test --profiles-dir $profilesDir --project-dir $baseDir` is run
    And `fal run --profiles-dir $profilesDir --project-dir $baseDir` is run
    Then the following scripts are ran:
      | agent_wait_time.after.py |
