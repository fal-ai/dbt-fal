Feature: __name__ should be __main__
  Background: Project Setup
    Given the project 002_jaffle_shop

  Scenario: main check should be present
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir --select +middle_2+
      """
    Then the following scripts are ran:
      | middle_2.middle_script.py |
    And the script middle_2.middle_script.py output file has the lines:
      | top name: __main__ | inner name: main_check_2 | passed main if |
