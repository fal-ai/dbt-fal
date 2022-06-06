Feature: `flow run` command with py nodes in the middle
  Background: Project Setup
    Given the project 002_jaffle_shop
    When the data is seeded

  Scenario: fal flow run command with selectors
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
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select middle_1+ --experimental-flow
      """
    Then the following models are calculated:
      | middle_1 | middle_2 | after_middle |
    And the following scripts are ran:
      | middle_2.middle_script.py |

  Scenario: fal flow run with or without experimental flag sends status information to after script updated
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select +middle_script.py --experimental-flow
      """
    Then the following scripts are ran:
      | middle_2.middle_script.py |
    And the script middle_2.middle_script.py output file has the lines:
      | Status: success |

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select middle_script.py --experimental-flow
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
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models
      """
    And the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select model_a+,model_c+ --experimental-models
      """
    Then the following models are calculated:
      | model_d |
    And the following scripts are ran:
      | model_c.after.py | model_e.after.py |

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
      | middle_2.middle_script.py |
