Feature: Structured Hooks
  Background: Project Setup
    Given the project 013_structured_hooks

  Scenario: Run a mix of structured/unstructured hooks
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select model_a
      """
    Then the following models are calculated:
      | model_a |
    And the following scripts are ran:
      | model_a.pre_hook_1.py | model_a.pre_hook_2.py | model_a.add.py | model_a.post_hook_1.py | model_a.post_hook_2.py | model_a.sub.py | model_a.types.py |
    And the script model_a.add.py output file has the lines
      | Calculation result: 5 |
    And the script model_a.sub.py output file has the lines
      | Calculation result: 3 |
    And the script model_a.types.py output file has the lines
      | Arguments: number=5, text='type', sequence=[1, 2, 3], mapping={'key': 'value'} |

  Scenario: Run isolated hooks
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select model_b
      """
    Then the following models are calculated:
      | model_b |
    And the following scripts are ran:
      | model_b.local_hook.py | model_b.funny_hook.py | model_b.check_imports.py |
    And the script model_b.funny_hook.py output file has the lines
      | PyJokes version: 0.6.0 |

  Scenario: Run isolated models
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select model_c model_d model_e
      """
    Then the following models are calculated:
      | model_c | model_d.py | model_e.py |
    And the following scripts are ran:
      | model_c.check_imports.py | model_d.check_imports.py | model_e.check_imports.py | model_e.joke_version.py | model_e.funny_hook.py |
    And the script model_d.py output file has the lines
      | PyJokes version: 0.5.0 |
    And the script model_e.py output file has the lines
      | PyJokes version: 0.6.0 |
    And the script model_e.funny_hook.py output file has the lines
      | PyJokes version: 0.5.0 |
    And the script model_e.joke_version.py output file has the lines
      | PyJokes version: 0.6.0 |

  Scenario: Run local hooks on isolated models
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select model_f
      """
    Then the following models are calculated:
      | model_f.py |
    And the following scripts are ran:
      | model_f.environment_type.py | model_f.environment_type_2.py | model_f.environment_type_3.py |
    And the script model_f.environment_type.py output file has the lines
      | Environment: local |
    And the script model_f.environment_type_2.py output file has the lines
      | Environment: local |
    And the script model_f.environment_type_3.py output file has the lines
      | Environment: venv |

  # Since conda requires an external installation step that we don't
  # automatically do in (at least not yet), we can't assume all testing
  # environments has it so it is guarded by a tag.
  #
  # We will check whether conda is available on runtime, and if so, we'll
  # run the test. Otherwise, we'll skip it.
  @requires-conda
  Scenario: Run local hooks on isolated models with conda
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select model_g
      """
    Then the following models are calculated:
      | model_g |
    And the following scripts are ran:
      | model_g.check_imports.py | model_g.environment_type.py | model_g.environment_type_2.py | model_g.environment_type_3.py |
    And the script model_g.environment_type.py output file has the lines
      | Environment: conda |
    And the script model_g.environment_type_2.py output file has the lines
      | Environment: venv |
    And the script model_g.environment_type_3.py output file has the lines
      | Environment: local |
