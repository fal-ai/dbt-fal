Feature: `run` command with a custom profile target
  Background:
    Given the project 007_run_with_custom_target
    When the data is seeded

  Scenario: fal run works with custom target
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --target custom
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py | zendesk_ticket_data.after.py |

  Scenario: fal run works after selected dbt model run with custom target
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --models agent_wait_time --target custom
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py |
    Then the following scripts are not ran:
      | zendesk_ticket_data.after.py |

  Scenario: fal run works with model selection with custom target
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --target custom
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --models zendesk_ticket_data
      """
    Then the following scripts are ran:
      | zendesk_ticket_data.after.py |
    Then the following scripts are not ran:
      | agent_wait_time.after.py |

  Scenario: fal run works with script selection with custom target
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --target custom
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --script fal_scripts/after.py
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py |

  Scenario: when false script is selected, nothing runs with custom target
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --target custom
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --script fal_scripts/notthere.py
      """
    Then no scripts are run
