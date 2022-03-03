---
sidebar_position: 3
---

# Model scripts selection

For node selection, `fal` handles [dbt's selection flags](https://docs.getdbt.com/reference/node-selection/syntax) for `dbt run`:

```
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
$ fal flow run --select model_alpha model_beta
... | Starting fal run for following models and scripts:
model_alpha: script.py
model_beta: script.py, other.py
```
