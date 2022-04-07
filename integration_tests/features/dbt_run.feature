Feature: `fal run` works with dbt test command
  Scenario: fal run works after dbt test
    Given the project 000_fal_run
    When the data is seeded
    And the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following shell command is invoked:
      """
      dbt test --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py |
