Feature: `flow run` command
  Scenario: fal flow run command with selectors
    Given the project 001_flow_run_with_selectors
    When fal flow is invoked with following arguements:
    """
    --profiles-dir $baseDir --project-dir $baseDir --select load_data.py+
    """
    Then the following models are calculated:
    | filtered_clusters |
    And the following scripts are ran:
    | transform.load_data.py | dataset.clustering.py |
    And there are no errors
