# Running Python in a dbt project

fal extends dbt's functionality to handle Python scripts before and after dbt runs. This enables you to use your data in a familiar way like you couldn't before.

This is done using the cli `fal flow run`.

Under the hood, the workflow is run in 3 parts:

1. Runs the pre-hook Python scripts assgined as `before` scripts
2. Runs the dbt models
3. Runs the post-hook Python scripts assgined as `after` scripts

This can be done individually in parts with the command [`fal run`](fal-run.md)

```
$ fal run --before
$ dbt run
$ fal run
```
