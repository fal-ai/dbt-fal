# Model scripts selection

By default, the `fal run` command runs the Python scripts as a post-hook, **only** on the models that were run on the last `dbt run`; that means that if you are using model selectors, `fal` will only run on the models `dbt` ran. To achieve this, fal needs the dbt-generated file `run_results.json` available.

If you are running `fal` in a clean environment (no `run_results.json` available) or just want to specify which models you want to run the scripts for, `fal` handles [dbt's selection flags](https://docs.getdbt.com/reference/node-selection/syntax) for `dbt run` as well as offering an extra flag for just running _all_ models:

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
