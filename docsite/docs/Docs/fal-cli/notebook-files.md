---
sidebar_position: 10
---

# Scripts in .ipynb files
You can use Jupyter notebook files as `fal` scripts.

## `init_fal` magic command
In order to access fal functions such as `ref` and `write_to_model` from within notebook runtime, you can use the `init_fal` magic command. 

First, you need to import `init_fal` within a Python cell:

```python
from faldbt.magics import init_fal
```

Now, you can initialize `fal` in your notebook:

```
%init_fal project_dir=project_dir profiles_dir=profiles_dir default_model_name=my_model
```

`init_fal` requires three inline arguments:

- `project_dir`: path to the dbt project directory
- `profiles_dir`: path to the dbt profiles directory
- `default_model_name`: the model name that will be used in `write_to_model`, applies only in notebook runtime.

Once executed, you can use fal functions in your notebook's Python cells:

```python
my_df = ref('some_model')

# We made some predictions and stored them in `my_predictions`

write_to_model(my_predictions)
```
Note that the `default_model_name` is only active during notebook runtime. When the script is run with `fal run` or `fal flow run`, fal will determine the model to write to according to the relevant `schema.yml` file. In fal runtime, the `init_fal` line is ignored. 

You can specify a .ipynb file the same way as a regular Python file:

```yaml
models:
  - name: some_model
    meta:
      fal:
        scripts:
            - my_notebook.ipynb
```
