Feature: `run` command
  Scenario: fal run works
    Given `dbt run --profiles-dir .` is run
    When `fal run --profiles-dir .` is run
    Then scripts are run for all models
    And outputs for all models contain run results

  Scenario: fal run works after selected dbt model run
    Given `dbt run --profiles-dir . --models agent_wait_time` is run
    When `fal run --profiles-dir .` is run
    Then scripts are run for agent_wait_time
    And outputs for agent_wait_time contain run results
    But zendesk_ticket_data scripts are skipped

  Scenario: fal run works on selected models
    Given `dbt run --profiles-dir .` is run
    When `fal run --profiles-dir . --model zendesk_ticket_data` is run
    Then scripts are run for zendesk_ticket_data
    And outputs for zendesk_ticket_data contain run results
    But agent_wait_time scripts are skipped

  Scenario: fal run works with script selection
    Given `dbt run --profiles-dir .` is run
    When `fal run --profiles-dir . --script fal_scripts/john_test.py` is run
    Then scripts are run for john_table
    But zendesk_ticket_data scripts are skipped

  Scenario: when false script is selected, nothing runs
    Given `dbt run --profiles-dir .` is run
    When `fal run --profiles-dir . --script fal_scripts/notthere.py` is run
    Then all model scripts are skipped
