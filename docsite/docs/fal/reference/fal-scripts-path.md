---
sidebar_position: 1
---

# Variable `fal-scripts-path`

In order to find scripts in your project, fal uses the `fal-scripts-path` [dbt variable](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-variables#defining-variables-in-dbt_projectyml).

Let's consider the following project structure:

```
.
├── dbt_project.yml
├── models
│   ├── schema.yml
│   └── some_model.sql
└── scripts
    ├── after.py
    ├── before.py
    └── utils
        ├── my_utils.py
        └── process
            └── process_df.py
```

By default `fal-scripts-path` is the [dbt project directory](https://docs.getdbt.com/reference/dbt_project.yml) (where the `dbt_project.yml` is located).

But it can be changed by setting in a dbt [var](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-variables#defining-variables-in-dbt_projectyml). An example of setting the variable would be:

```yaml
name: "fal_test"
version: "1.0.0"
config-version: 2

vars:
  fal-scripts-path: "scripts"
```

## Script reference in `schema.yml`

Use the `fal-scripts-path` dbt variable as a base directory when [associating to your models](../Docs/fal-cli/model-scripts.md) to python scripts .

Referencing a script in your `schema.yml` with the default `fal-scripts-path` value looks like:

```yaml
version: 2

models:
  - name: some_model
    meta:
      fal:
        scripts:
          before:
            # searches in `./` because var has default value
            - scripts/before.py
        post-hook:
          - scripts/after.py
```

But if the `fal-scritps-path` value is changed to `scripts`, like specified above, the `schema.yml` would be:

```yaml
version: 2

models:
  - name: some_model
    meta:
      fal:
        scripts:
          before:
            # searches in `./scripts/` because of var
            - before.py
        post-hook:
          - after.py
```

## Script importing during runs

For larger scripts or repeated functionality, you may decide to have several Python files with functions to be imported into your [fal scripts](../Docs/fal-cli/model-scripts.md).

The [`fal-scripts-path`](#script-path-for-in-a-dbt-project) variable refers to the base directory from which you do your imports. Changing `fal-scripts-path` also changes the base import directory.

For example; importing a script with the default `fal-scripts-path` value looks like:

```py
# Searching from the top level: include `script` directory in path
import scripts.utils.my_utils as my_utils
from scripts.utils.process.process_df import some_func
```

Changing the `fal-scripts-path` value to `scripts`, like specified above, would require `import` changes:

```py
# Searching from the `scripts` directory
import utils.my_utils as my_utils
from utils.process.process_df import some_func
```
