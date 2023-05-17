@cloud
Feature: Basic dbt commands
  Background: Project Setup
    Given the project env_project

  @run
  Scenario: Run dbt --version
    When the following shell command is invoked:
      """
      dbt --version
      """
    Then there should be no errors

  @run
  Scenario: Run dbt debug
    When the following shell command is invoked:
      """
      dbt debug --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then there should be no errors
