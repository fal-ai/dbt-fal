Feature: `run` command
  Background:
    Given the project 000_fal_run
    When the data is seeded

  Scenario: fal run works
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py | zendesk_ticket_data.after.py |

  Scenario: fal run works after selected dbt model run
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --models agent_wait_time
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py |
    Then the following scripts are not ran:
      | zendesk_ticket_data.after.py |

  Scenario: fal run works with model selection
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --models zendesk_ticket_data
      """
    Then the following scripts are ran:
      | zendesk_ticket_data.after.py |
    Then the following scripts are not ran:
      | agent_wait_time.after.py |

  Scenario: fal run works with script selection
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --script fal_scripts/after.py
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py |

  Scenario: fal flow run provides model aliases
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select agent_wait_time+
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py |
    And the script agent_wait_time.after.py output file has the lines:
      | Model alias is wait_time |

  Scenario: when false script is selected, nothing runs
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --script fal_scripts/notthere.py
      """
    Then no scripts are run
