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

  Scenario: fal run works with model selection
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --model zendesk_ticket_data
      """
    Then the following scripts are ran:
      | zendesk_ticket_data.after.py |

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
