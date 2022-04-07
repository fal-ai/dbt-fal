---
sidebar_position: 8
---

# Custom script directory
By default, fal assumes that the directory where scripts are stored is the root directory of a dbt project. You can change this. Within your `dbt_project.yml`, you can add a custom variable that will tell fal where to look for scripts:
```yaml
...
vars:
  fal-scripts-path: "my_scripts_dir"
```
Now, if a script (`after.py`) is put in `my_scripts_dir` directory within your dbt project, you can refer to it by name in `schema.yml`:
```yaml
models:
  - name: some_model
    meta:
      fal:
        scripts:
            - after.py
```
Similarly, you can do it when using the `--scripts` flag:
```
fal run --scripts after.py
```

## Local imports in your fal scripts
The directory containing fal scripts is temporarily added to `sys.path` during runtime. This means that you can import local modules in your scripts directory. Say we have the following structure in our scripts directory:

```
└── my_scripts_dir
    ├── after.py
    └── my_utils
        ├── custom_functions.py
        └── message
            └── slack.py
```

This will allow you to do local imports in `after.py`:

```python
from my_utils.message.slack import send_slack_message
...
send_slack_message(my_message)
...
```

Also, within `slack.py` you can do relative imports:

```python
from ..custom_functions import format_string
...
formatted = format_string(my_string)
...
```

Note that in this example, `my_scripts_dir` is not itself loaded as a module, so a similar import wouldn't work from `custom_functions.py`.
