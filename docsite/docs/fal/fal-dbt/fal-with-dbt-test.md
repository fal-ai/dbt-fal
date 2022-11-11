---
sidebar_position: 6
---
# Run scripts with dbt test results

If you run `dbt test`, you may want to run some scripts based on the results of the tests (e.g. to notify failures).

This can be achieved by manually invoking

```
$ dbt test
$ fal run
```

The fal run command also processes test results, and they will be available in the `context` variable.

```py
for test in context.current_model.tests:
    if test.status != 'sucess':
        notify(test.modelname, test.name, test.column, test.status)
```

*****

Better integration of fal flow with dbt test should be done in the future.
