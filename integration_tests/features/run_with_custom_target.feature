@broken_profile
Feature: `run` command with a custom profile target
  Background:
    Given the project 000_fal_run
    When the data is seeded to custom target in profile directory profiles/broken

  Scenario: fal run works with custom target
    When the following shell command is invoked:
      """
      dbt run --profiles-dir profiles/broken --project-dir $baseDir --select agent_wait_time zendesk_ticket_data --target custom
      """
    And the following command is invoked:
      """
      fal run --profiles-dir profiles/broken --project-dir $baseDir
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py | zendesk_ticket_data.post_hook.py | zendesk_ticket_data.post_hook2.py |

  Scenario: fal run works after selected dbt model run with custom target
    When the following shell command is invoked:
      """
      dbt run --profiles-dir profiles/broken --project-dir $baseDir --models agent_wait_time --target custom
      """
    And the following command is invoked:
      """
      fal run --profiles-dir profiles/broken --project-dir $baseDir
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py |

  Scenario: fal run works with model selection with custom target
    When the following shell command is invoked:
      """
      dbt run --profiles-dir profiles/broken --project-dir $baseDir --target custom
      """
    And the following command is invoked:
      """
      fal run --profiles-dir profiles/broken --project-dir $baseDir --models zendesk_ticket_data
      """
    Then the following scripts are ran:
      | zendesk_ticket_data.post_hook.py | zendesk_ticket_data.post_hook2.py |

  Scenario: fal run works with script selection with custom target
    When the following shell command is invoked:
      """
      dbt run --profiles-dir profiles/broken --project-dir $baseDir --target custom --select agent_wait_time
      """
    And the following command is invoked:
      """
      fal run --profiles-dir profiles/broken --project-dir $baseDir --script fal_scripts/after.py
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py |
