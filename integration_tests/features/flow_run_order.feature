Feature: `flow run` command with ensured order
  Background: Project Setup
    Given the project 002_jaffle_shop
    When the data is seeded

  Scenario: fal flow run order without --experimental-flow
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select middle_1+
      """
    Then the following nodes are calculated in the following order:
      | middle_1 | middle_2 | after_middle | middle_2.middle_script.py |

  Scenario: fal flow run order with --experimental-flow
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-flow --select middle_1+
      """
    Then the following nodes are calculated in the following order:
      | middle_1 | middle_2 | middle_2.middle_script.py | after_middle |

  Scenario: fal flow run order with pure python models
    Given the project 008_pure_python_models
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --exclude broken_model
      """
    Then the following nodes are calculated in the following order:
      | model_a | model_a.after.py | model_b | model_c.py | model_c.post_hook.py | model_c.after.py | model_d | model_e.ipynb | model_e.after.py |

  Scenario: fal flow run order with pure python models on a selection
    Given the project 008_pure_python_models
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select model_c
      """
    Then the following nodes are calculated in the following order:
      | model_c.py | model_c.post_hook.py |
