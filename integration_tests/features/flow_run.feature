Feature: `flow run` command

  Scenario: post hooks cannot be selected
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select check_extra.py
      """
    Then no models are calculated
    And no scripts are run

  Scenario: fal flow run command with selectors
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select before.py+
      """
    Then the following models are calculated:
      | agent_wait_time | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 | model_c |
    And the following scripts are ran:
      | agent_wait_time.before.py | agent_wait_time.after.py | model_c.before.py |

  Scenario: fal flow run command with selectors
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select before.py
      """
    Then no models are calculated
    And the following scripts are ran:
      | agent_wait_time.before.py | model_c.before.py |

  Scenario: fal flow run command with complex selectors
    Given the project 001_flow_run_with_selectors
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select +intermediate_model_3 --threads 1
      """
    Then the following models are calculated:
      | agent_wait_time | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 | model_a | model_b | model_c |

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select intermediate_model_1+ --threads 1
      """
    Then the following models are calculated:
      | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 |

  Scenario: fal flow run command with plus operator with number selectors
    Given the project 001_flow_run_with_selectors
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select 2+intermediate_model_3 --threads 1
      """
    Then the following models are calculated:
      | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 |

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select intermediate_model_1+1 --threads 1
      """
    Then the following models are calculated:
      | intermediate_model_1 | intermediate_model_2 |

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select intermediate_model_1+0 --threads 1
      """
    Then the following models are calculated:
      | intermediate_model_1 |

  Scenario: fal flow run command with selectors
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    And the file $baseDir/models/new_model.sql is created with the content:
      """
      select * 1
      """
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select agent_wait_time --threads 1
      """
    Then the following models are calculated:
      | agent_wait_time |

  Scenario: fal flow run command with state selector without state
    Given the project 001_flow_run_with_selectors
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --threads 1
      """
    And the file $baseDir/models/new_model.sql is created with the content:
      """
      select 1
      """
    Then the following command will fail:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select state:new --threads 1
      """
    And no models are calculated

  Scenario: fal flow run command with state selector and with state
    Given the project 001_flow_run_with_selectors
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --threads 1
      """
    And state is stored in old_state
    And the file $baseDir/models/new_model.sql is created with the content:
      """
      select cast(NULL as numeric) AS my_float
      """
    And the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select state:new --state $tempDir/old_state --threads 1
      """
    Then the following models are calculated:
      | new_model |

  Scenario: fal flow run with an error in before
    Given the project 003_scripts_with_errors
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select before.py
      """
    Then it throws an exception RuntimeError with message 'Error in scripts'

  Scenario: fal flow run with an error in after
    Given the project 003_scripts_with_errors
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select after.py
      """
    Then it throws an exception RuntimeError with message 'Error in scripts'

  @TODO-duckdb
  Scenario: fal flow run with an error in dbt run
    Given the project 003_scripts_with_errors
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select some_model
      """
    Then it throws an exception RuntimeError with message 'Error running dbt run'

  Scenario: fal flow run command with selectors with complex selectors
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select tag:daily
      """
    Then the following models are calculated:
      | agent_wait_time |

  Scenario: fal flow run command with complex selectors and children
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select tag:daily+
      """
    Then the following models are calculated:
      | agent_wait_time | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 |
    And the following scripts are ran:
      | agent_wait_time.after.py |

  Scenario: fal flow run post-hooks run with parent model without +
    Given the project 001_flow_run_with_selectors
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select zendesk_ticket_data --threads 1
      """
    Then the following models are calculated:
      | zendesk_ticket_data |
    And the following scripts are ran:
      | zendesk_ticket_data.check_extra.py |

  Scenario: fal flow run command with vars
    Given the project 001_flow_run_with_selectors
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select zendesk_ticket_data --threads 1
      """
    Then the following models are calculated:
      | zendesk_ticket_data |
    And the script zendesk_ticket_data.check_extra.py output file has the lines:
      | no extra_col |

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select zendesk_ticket_data --vars 'extra_col: true' --threads 1
      """
    Then the following models are calculated:
      | zendesk_ticket_data |
    And the script zendesk_ticket_data.check_extra.py output file has the lines:
      | extra_col: yes |

  Scenario: fal flow run command with exclude arg
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --exclude before.py
      """
    Then the following models are calculated:
      | agent_wait_time | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 | zendesk_ticket_data | model_b | model_a | model_c |
    And the following scripts are ran:
      | agent_wait_time.after.py | zendesk_ticket_data.check_extra.py |

  Scenario: fal flow run command with exclude arg with children
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --exclude before.py+
      """
    Then the following models are calculated:
      | zendesk_ticket_data | model_a | model_b |
    And the following scripts are not ran:
      | agent_wait_time.before.py | agent_wait_time.after.py |

  Scenario: fal flow run command with exclude arg and select arg
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select before.py+ --exclude after.py
      """
    Then the following models are calculated:
      | agent_wait_time | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 | model_c |
    And the following scripts are not ran:
      | agent_wait_time.after.py |

  Scenario: fal flow run command with select @
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select @agent_wait_time
      """
    Then the following models are calculated:
      | agent_wait_time | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 | model_a | model_b | model_c |
    And the following scripts are ran:
      | model_c.before.py | agent_wait_time.before.py | agent_wait_time.after.py |

  Scenario: fal flow run with @ in the middle
    Given the project 001_flow_run_with_selectors
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select @model_c
      """
    Then the following models are calculated:
      | agent_wait_time | intermediate_model_1 | intermediate_model_2 | intermediate_model_3 | model_a | model_b | model_c |
    And the following scripts are ran:
      | model_c.before.py | agent_wait_time.before.py |

  @broken_profile
  Scenario: fal flow run with target
    Given the project 001_flow_run_with_selectors
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir profiles/broken --project-dir $baseDir --select zendesk_ticket_data --threads 1
      """
    Then it throws an exception RuntimeError with message 'Error running dbt run'

    When the following command is invoked:
      """
      fal flow run --profiles-dir profiles/broken --project-dir $baseDir --select zendesk_ticket_data --threads 1 --target custom
      """
    Then the following models are calculated:
      | zendesk_ticket_data |

  Scenario: post hooks run after both successful and failed dbt models
    Given the project 003_scripts_with_errors
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --exclude before.py
      """
    Then it throws an exception RuntimeError with message 'Error running dbt run'
    And the following scripts are ran:
      | working_model.post_hook.py | some_model.post_hook.py |
    And the script working_model.post_hook.py output file has the lines:
      | Status: success |
    And the script some_model.post_hook.py output file has the lines:
      | Status: error |
