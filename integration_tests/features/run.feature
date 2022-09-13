Feature: `run` command
  Background:
    Given the project 000_fal_run
    When the data is seeded

  Scenario: fal run works
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --models agent_wait_time zendesk_ticket_data
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py | zendesk_ticket_data.post_hook.py | zendesk_ticket_data.post_hook2.py |

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

  Scenario: fal run works with pre-hooks
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --models zendesk_ticket_data --before
      """
    Then the following scripts are ran:
      | zendesk_ticket_data.pre_hook.py |

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
      | zendesk_ticket_data.post_hook.py | zendesk_ticket_data.post_hook2.py |

  Scenario: fal run works with script selection
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --models agent_wait_time
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --script fal_scripts/after.py
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py |

  @TODO-redshift
  # TODO: use real redshift instance for testing
  Scenario: fal run is aware of source freshness
    Given the project 010_source_freshness
    When the following shell command is invoked:
      """
      python $baseDir/load_freshness_table.py $baseDir $profilesDir
      """
    And the following shell command is invoked:
      """
      dbt source freshness --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --exclude '*'
      """
    Then the following scripts are ran:
      | GLOBAL.freshness.py |
    And the script GLOBAL.freshness.py output file has the lines:
      | (freshness_test, freshness_table) pass          |
      | (freshness_test, freshness_other) runtime error |

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

  Scenario: fal run provides model aliases
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --models agent_wait_time zendesk_ticket_data
      """

    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py | zendesk_ticket_data.post_hook.py | zendesk_ticket_data.post_hook2.py |
    And the script agent_wait_time.after.py output file has the lines:
      | Model alias without namespace is wait_time |

  Scenario: when false script is selected, nothing runs
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --models agent_wait_time
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --script fal_scripts/notthere.py
      """
    Then no scripts are run

  Scenario: Post hooks with write_to_model will fail
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --models some_model
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then it throws an exception Exception with message 'Error in scripts'

  Scenario: Post hooks with write_to_source will fail
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir --models some_other_model
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then it throws an exception Exception with message 'Error in scripts'

