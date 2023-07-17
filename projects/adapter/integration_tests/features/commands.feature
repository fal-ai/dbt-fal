@cloud
Feature: Basic dbt commands
  Background: Project Setup
    Given the project env_project

  Scenario: Run dbt --version
    When the following shell command is invoked:
      """
      dbt --version
      """
    Then there should be no errors

  Scenario: Run dbt debug
    When the following shell command is invoked:
      """
      dbt debug --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then there should be no errors
