---
sidebar_position: 4
---

# Selecting `before` and `after` scripts

For node selection, `fal` handles [dbt's selection flags](https://docs.getdbt.com/reference/node-selection/syntax):

```
-s SELECT [SELECT ...], --select SELECT [SELECT ...]
                      Specify the nodes to include.
-m SELECT [SELECT ...], --models SELECT [SELECT ...]
                      Specify the nodes to include.
--exclude EXCLUDE [EXCLUDE ...]
                      Specify the nodes to exclude.
--selector SELECTOR   The selector name to use, as defined in selectors.yml
```

You may pass more than one selection at a time and use graph operators:

```bash
$ fal flow run --select model_alpha+ model_beta+
Executing command: dbt --log-format json run --project-dir /Users/matteo/Projects/fal/fal_dbt_examples --profiles-dir . --exclude lombardia_covid miami
Running with dbt=1.0.3
...
1 of 2 START view model dbt_matteo.model_alpha......... [RUN]
2 of 2 START table model dbt_matteo.model_beta......... [RUN]
1 of 2 OK created view model dbt_matteo.model_alpha.... [OK in 2.71s]
2 of 2 OK created table model dbt_matteo.model_beta.... [CREATE TABLE (10.0 rows, 2.6 KB processed) in 4.32s]
Finished running 1 view models, 1 table models in 11.32s.
Completed successfully
Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
...
... | Starting fal run for following models and scripts:
model_alpha: script.py
model_beta: script.py, other.py
...
```

> Please note that individual `post-hook`s can not be selected, they will be ran if the underlying
> model is selected.
