---
sidebar_position: 10
---

# Use .ipynb files as fal hooks 

You can use Jupyter notebook files as `fal` scripts.

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
