---
sidebar_position: 6
---

# The fal run command

> **NOTICE**: The `fal run` command was previously the only way to run scripts. We still support the `fal run` behavior but we recommend the [`fal flow run`](.) for more capabilities.

By default, the `fal run` command runs the Python scripts as a post-hook, **only** on the models that were run on the last `dbt run`; that means that if you are using model selection with `dbt`, `fal` will only run on the models `dbt` ran. To achieve this, fal needs the dbt-generated file [`run_results.json`](https://docs.getdbt.com/reference/artifacts/run-results-json) available.

If you are running `fal` without a `run_results.json` available, or just want to specify which models you want to run the scripts for, `fal` handles [dbt's selection flags](https://docs.getdbt.com/reference/node-selection/syntax) for `dbt run` as well as offering an extra flag to ignore the run results and run _all_ models:

```
--all                 Run scripts for all models.
-s SELECT [SELECT ...], --select SELECT [SELECT ...]
                      Specify the nodes to include.
-m SELECT [SELECT ...], --models SELECT [SELECT ...]
                      Specify the nodes to include.
--exclude EXCLUDE [EXCLUDE ...]
                      Specify the nodes to exclude.
--selector SELECTOR   The selector name to use, as defined in selectors.yml
```

You may pass more than one selection at a time:

```bash
$ fal run --select model_alpha model_beta
... | Starting fal run for following models and scripts:
model_alpha: script.py
model_beta: script.py, other.py
```

## Running scripts before dbt runs

The `--before` flag let's users run scripts before their dbt runs.

Given the following schema.yml:

```
models:
  - name: boston
    description: Ozone levels
    config:
      materialized: table
    meta:
      owner: "@meder"
      fal:
      	scripts:
          before:
            - fal_scripts/postgres.py
  	      after:
            - fal_scripts/slack.py
```

`fal run --before` will run `fal_scripts/postgres.py` script regardless if dbt has calculated the boston model or not. `fal run` without the `--before` flag, will run `fal_scripts/slack.py`, but only if boston model is already calculated by dbt.

A typical workflow involves running `dbt run` after invoking `fal run --before`.

```bash
$ fal run --before --select boston
$ dbt run --select boston
```
