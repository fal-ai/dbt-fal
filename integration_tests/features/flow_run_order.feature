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
      """
      1. middle_1
      2. middle_2
      3. after_middle,
      4. middle_2.middle_script.py
      """

  Scenario: fal flow run order with --experimental-flow
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-flow --select middle_1+
      """
    Then the following nodes are calculated in the following order:
      """
      1. middle_1
      2. middle_2
      3. middle_2.middle_script.py
      4. after_middle
      """

  Scenario: fal flow run order with pure python models
    Given the project 008_pure_python_models
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --exclude broken_model
      """
    Then the following nodes are calculated in the following order:
      """
      1. model_a
      2. model_a.after.py
      3. model_b
      4. model_c.py
      5. model_c.post_hook.py, model_c.after.py
      6. model_d, model_e.ipynb, model_e.after.py
      """

  Scenario: fal flow run order with pure python models on a selection
    Given the project 008_pure_python_models
    When the data is seeded

    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --experimental-models --select model_c
      """
    Then the following nodes are calculated in the following order:
      """
      1. model_c.py
      2. model_c.post_hook.py
      """
