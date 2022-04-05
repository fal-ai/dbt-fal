Feature: `run` command
  Background:
    Given the project 000_fal_run
    When the data is seeded

  Scenario: fal run works
    When `dbt run --profiles-dir $profilesDir --project-dir $baseDir` is run
    And `fal run --profiles-dir $profilesDir --project-dir $baseDir` is run
    Then the following scripts are ran:
    | agent_wait_time.after.py | zendesk_ticket_data.after.py |

  Scenario: fal run works after selected dbt model run
    When `dbt run --profiles-dir $profilesDir --project-dir $baseDir --models agent_wait_time` is run
    And `fal run --profiles-dir $profilesDir --project-dir $baseDir` is run
    Then the following scripts are ran:
    | agent_wait_time.after.py |

  Scenario: fal run works with model selection
    When `dbt run --profiles-dir $profilesDir --project-dir $baseDir` is run
    And `fal run --profiles-dir $profilesDir --project-dir $baseDir --model zendesk_ticket_data` is run
    Then the following scripts are ran:
    | zendesk_ticket_data.after.py |

  Scenario: fal run works with script selection
    When `dbt run --profiles-dir $profilesDir --project-dir $baseDir` is run
    And `fal run --profiles-dir $profilesDir --project-dir $baseDir --script fal_scripts/after.py` is run
    Then the following scripts are ran:
    | agent_wait_time.after.py |

  Scenario: when false script is selected, nothing runs
    When `dbt run --profiles-dir $profilesDir --project-dir $baseDir` is run
    And `fal run --profiles-dir $profilesDir --project-dir $baseDir --script fal_scripts/notthere.py` is run
    Then no scripts are run
