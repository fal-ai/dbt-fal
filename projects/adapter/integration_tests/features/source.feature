@TODO-duckdb
@TODO-bigquery
Feature: dbt-fal can query sources
  Background: Project Setup
    Given the project source_freshness

  Scenario: Run a Python model that queries a source in local environment
    When the following shell command is invoked:
      """
      python $baseDir/load_freshness_table.py $baseDir $profilesDir
      """
    And the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following models are calculated in order:
      | model_a |

  @cloud
  Scenario: Run a Python model that queries a source with Isolate Cloud
    When the following shell command is invoked:
      """
      python $baseDir/load_freshness_table.py $baseDir $profilesDir
      """
    And the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir -t prod
      """
    Then the following models are calculated in order:
      | model_a |
