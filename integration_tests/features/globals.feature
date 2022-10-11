Feature: global scripts
  Scenario: fal run triggers globals by default
    Given the project 004_globals
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | some_model.after.py | GLOBAL.after.py |

  Scenario: fal run doesn't trigger globals with select flag
    Given the project 004_globals
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --select some_model
      """
    Then the following scripts are ran:
      | some_model.after.py |

  Scenario: fal run triggers globals with select and globals flags
    Given the project 004_globals
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --select some_model --globals
      """
    Then the following scripts are ran:
      | some_model.after.py | GLOBAL.after.py |

  Scenario: fal run triggers globals by default with before
    Given the project 004_globals
    When the following shell command is invoked:
      """
      dbt run --profiles-dir $profilesDir --project-dir $baseDir
      """
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --before
      """
    Then the following scripts are ran:
      | GLOBAL.before.py | GLOBAL.before_b.py | some_model.before.py |

  Scenario: --before script selection doesn't run globals
    Given the project 004_globals
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --before --script fal_scripts/before.py
      """
    Then the following scripts are ran:
      | some_model.before.py |

  Scenario: global before scripts are run with --globals flag and script selection
    Given the project 004_globals
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --before --script fal_scripts/before.py --globals
      """
    Then the following scripts are ran:
      | GLOBAL.before.py | some_model.before.py |

  Scenario: Fal selects global and not-global scripts
    Given the project 004_globals
    When the following command is invoked:
      """
      fal run --profiles-dir $profilesDir --project-dir $baseDir --before --script fal_scripts/before.py --globals
      """
    Then the following scripts are ran:
      | GLOBAL.before.py | some_model.before.py |

  Scenario: fal flow run does not trigger globals
    Given the project 004_globals
    When the following command is invoked:
      """
      fal flow run --profiles-dir $profilesDir --project-dir $baseDir
      """
    Then the following scripts are ran:
      | some_model.before.py | some_model.after.py |
