# Lifecycle and State Management

By default, the `fal run` command runs the Python scripts as a post-hook, **only** on the models that were ran on the last `dbt run` (So if you are using model selectors, `fal` will only run on the selected models).

If you want to run all Python scripts regardless, you can do so by using the `--all` flag with the `fal` CLI:

```bash
$ fal run --all
```
