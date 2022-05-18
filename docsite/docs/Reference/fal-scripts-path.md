---
sidebar_position: 1
---

# Variable `fal-scripts-path`

fal uses the `fal-scripts-path` [dbt variable](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-variables#defining-variables-in-dbt_projectyml) to scripts in your project.

Let's consider the following file structure for some examples:
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

But it can be chagend in the [vars](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-variables#defining-variables-in-dbt_projectyml). An example of setting the variable would be:
```yaml
name: "fal_test"
version: "1.0.0"
config-version: 2

vars:
  fal-scripts-path: "scripts"

...
```

## Script reference in `schema.yml`

The Python scripts to be [associated to your models](../Docs/fal-cli/model-scripts.md) use the `fal-scripts-path` dbt variable as a base directory to search the files.

Meaning that referencing a script in your `schema.yml` with the default `fal-scripts-path` value would look like:
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
          after:
            - scripts/after.py
```

But if changed the `fal-scritps-path` value to `scripts`, like specified above, the `schema.yml` would be:
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
          after:
            - after.py
```


## Script importing during runs

For larger scripts or repeated functionality, you may decide to have several Python files with functions to be imported into your [fal scripts](../Docs/fal-cli/model-scripts.md).

Modifying the [`fal-scripts-path`](#script-path-for-in-a-dbt-project) var also affects how importing a separate Python script from your script works. The base directory for searching scripts during import is `fal-scripts-path`.

Meaning that importing a script with the default `fal-scripts-path` value would look like:
```py
# Searching from the top level: include `script` directory in path
import scripts.utils.my_utils as my_utils
from scripts.utils.process.process_df import some_func
```

But if changed the `fal-scripts-path` value to `scripts`, like specified above, the import would look like:
```py
# Searching from the `scripts` directory
import utils.my_utils as my_utils
from utils.process.process_df import some_func
```
