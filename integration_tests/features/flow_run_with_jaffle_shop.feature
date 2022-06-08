Feature: `flow run` command
  Background: Project Setup
    Given the project 002_jaffle_shop
    When the data is seeded

  Scenario: fal flow run command with selectors
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select load_data.py+ stg_orders stg_payments
      """
    Then the following models are calculated:
      | stg_customers | customers | stg_orders | stg_payments |
    And the following scripts are ran:
      | stg_customers.load_data.py | customers.send_slack_message.py |
