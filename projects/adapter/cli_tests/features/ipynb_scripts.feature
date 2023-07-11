@TODO-postgres
@TODO-fal
@TODO-bigquery
@TODO-snowflake
@TODO-redshift
@TODO-duckdb
@TODO-athena
Feature: fal works with ipynb features

  Scenario: fal flow run command for ipynb scripts
    Given the project 007_ipynb_scripts
    When the data is seeded
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following models are calculated:
      | agent_wait_time | zendesk_ticket_data |
    And the following scripts are ran:
      | agent_wait_time.before.py | agent_wait_time.after.py | zendesk_ticket_data.check_extra.py | zendesk_ticket_data.my_notebook.py |

  Scenario: fal flow run command with vars
    Given the project 007_ipynb_scripts
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select zendesk_ticket_data+
      """
    Then the following models are calculated:
      | zendesk_ticket_data |
    And the script zendesk_ticket_data.check_extra.py output file has the lines:
      | no extra_col |
    And the script zendesk_ticket_data.my_notebook.py output file has the lines:
      | no extra_col |

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select zendesk_ticket_data+ --vars 'extra_col: true'
      """
    Then the following models are calculated:
      | zendesk_ticket_data |
    And the script zendesk_ticket_data.check_extra.py output file has the lines:
      | extra_col: yes |
    And the script zendesk_ticket_data.my_notebook.py output file has the lines:
      | extra_col: yes |

  Scenario: fal run works
    Given the project 007_ipynb_scripts
    When the data is seeded

    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | agent_wait_time.after.py | zendesk_ticket_data.check_extra.py | zendesk_ticket_data.my_notebook.py |

  Scenario: fal run works with model selection
    Given the project 007_ipynb_scripts
    When the data is seeded

    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --models zendesk_ticket_data
      """
    Then the following scripts are ran:
      | zendesk_ticket_data.check_extra.py | zendesk_ticket_data.my_notebook.py |

  Scenario: fal run works with script selection
    Given the project 007_ipynb_scripts
    When the data is seeded

    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    And the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --script fal_scripts/notebooks/my_notebook.ipynb
      """
    Then the following scripts are ran:
      | zendesk_ticket_data.my_notebook.py |
