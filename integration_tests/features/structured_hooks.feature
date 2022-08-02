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
