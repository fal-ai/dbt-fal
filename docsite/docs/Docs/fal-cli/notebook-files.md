---
sidebar_position: 9
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
- `default_model_name`: the model name that this notebook is attached to in notebook runtime.

Once executed, you can use fal functions in your notebook's Python cells:

```python
my_df = ref('my_model')

write_to_model(my_predictions)
```

Note that the `default_model_name` is only active notebook runtime. When run using `fal run` or `fal flow run`, fal will determine the model according to the relevant `schema.yml` file. In fal runtime `init_fal` arguments are ignored. You can specify a .ipynb file the same way as a regular Python file:

```yaml
models:
  - name: some_model
    meta:
      fal:
        scripts:
            - my_notebook.ipynb

```
