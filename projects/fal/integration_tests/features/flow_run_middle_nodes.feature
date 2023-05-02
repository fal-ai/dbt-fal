Feature: `flow run` command with py nodes in the middle
  Background: Project Setup
    Given the project 002_jaffle_shop
    When the data is seeded

  Scenario: fal flow run command with selectors middle nodes
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select middle_1+
      """
    Then the following models are calculated:
      | middle_1 | middle_2 | after_middle |
    And the following scripts are ran:
      | middle_2.middle_script.py |

  Scenario: fal flow run command with selectors with experimental flag
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select middle_1+
      """
    Then the following models are calculated in order:
      | middle_1 | middle_2 | after_middle |
    And the following scripts are ran:
      | middle_2.middle_script.py |

  Scenario: fal flow run with or without experimental flag sends status information to after script updated
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select +middle_script.py
      """
    Then the following scripts are ran:
      | middle_2.middle_script.py |
    And the script middle_2.middle_script.py output file has the lines:
      | Status: success |

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select middle_script.py
      """
    Then the following scripts are ran:
      | middle_2.middle_script.py |
    And the script middle_2.middle_script.py output file has the lines:
      | Status: skipped |

  Scenario: fal flow run command with set intersection selector
    Given the project 008_pure_python_models
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --exclude broken_model
      """
    And the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select model_a+,model_c+
      """
    Then the following models are calculated in order:
      | model_c.py | model_d | model_e.ipynb |
    And the following scripts are ran:
      | model_c.after.py | model_e.pre_hook.py | model_e.post_hook.py | model_c.post_hook.py |

  Scenario: fal flow run command with triple intersection selectors on parents
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select +customers,+orders_forecast,+orders
      """
    Then the following models are calculated:
      | stg_orders | stg_payments |

  Scenario: fal flow run command with intersection and union mixed
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select +customers +middle_script.py
      """
    And the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select stg_customers+,stg_orders+,stg_payments+ middle_script.py
      """
    Then the following models are calculated:
      | customers |
    Then the following scripts are ran:
      | middle_2.middle_script.py | customers.send_slack_message.py |

  Scenario: fal flow run command with intersection and union mixed as a single string
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select +customers +middle_script.py
      """
    And the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select "stg_customers+,stg_orders+,stg_payments+ middle_script.py"
      """
    Then the following models are calculated:
      | customers |
    Then the following scripts are ran:
      | customers.send_slack_message.py | middle_2.middle_script.py |

  # This is technically an outlier case (in the following case, the actual intersection is model_b but
  # due to how DBT parses command line arguments the following works like a regular union) so we are
  # simply mirroring the exact behavior from DBT.
  Scenario: fal flow run command with quoted union groups with a distinct intersection operator
    Given the project 008_pure_python_models
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select "model_a model_b" , "model_b model_c"
      """
    Then the following models are calculated in order:
      | model_a | model_b | model_c.py |
