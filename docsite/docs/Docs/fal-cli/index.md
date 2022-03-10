# Running Python in a dbt project

fal extends dbt's functionality to run Python scripts before and after dbt models. With fal you can interact with dbt models in your Python scripts seamlessly.

This is done using the command `fal flow run`.

<!-- TODO: add a graph to show which nodes run when the example fal flow run is invoked -->

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
