@cloud
Feature: Basic dbt commands
  Background: Project Setup
    Given the project env_project

  Scenario: Run dbt --version
    When the following shell command is invoked:
      """
      dbt --version
      """

  Scenario: Run dbt debug
    When the following shell command is invoked:
      """
      dbt debug
      """
