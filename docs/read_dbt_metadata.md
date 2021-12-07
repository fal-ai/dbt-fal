Example 9: Read Dbt Model Metadata

Dbt supports a special `meta` tag in configuration definitions to allow engineers to to add arbitrary information to their schema.yml and sources.yml files.

This tag can be used in various ways, one common example is to [assign owners to the models](https://docs.getdbt.com/reference/resource-configs/meta#designate-a-model-owner).

```
version: 2

models:
  - name: users
    meta:
      owner: "@alice"
```

Dbt users can take advantage of `fal-dbt` to parse the configuration under the `meta` tag and use this data for other purposes. For example; to send slack notification to owners of models after their model completes a dbt run.

In this example we'll leave what to do with this data to the imagination of the reader, but go through how to parse the `meta` tag in a fal script.

## Install Fal

```bash
pip install fal
```

## Create a fal script

Now navigate to your dbt to project, create a directory for your fal scripts and create a python file inside that directory. [For example](https://github.com/fal-ai/fal_dbt_examples/tree/main/fal_scripts/list_owners_of_models.py) a directory named `fal_scripts` and a python file named `list_owners_of_models.py`.

In your python file you'll write the script that will parse the `meta` for all the models and print the owners to the console.

```
models = list_models()
for model in models:
    if model.meta:
        print(model.meta["owner"])
```

## Meta tag

Fal-dbt is also making use of the meta tag to configure when this script will run. This part will be different for every usecase. Currently there are two types of triggers, a script can be configured per model or globally for the whole project. In this example we want this script to run once, not for any specific model.

To configure this navigate to your `schema.yml` file or create one if you dont have one and add the following yaml entry.

```
fal:
  scripts:
    - fal_scripts/list_owners_of_models.py
```

so your `schema.yml` might look like:

```yaml
models:
  - name: boston
    meta:
      owner: "@ali"

  - name: los angeles
    meta:
      owner: "@gorkem"

fal:
  scripts:
    - fal_scripts/list_owners_of_models.py
```

## Run fal

After a successful dbt run invoke the fal cli.

```
dbt run

# Found 4 models, 0 tests, 0 snapshots, 0 analyses, 184 macros, 0 operations, 1 seed file, 0 sources, 0 exposures
# Completed successfully
# Done. PASS=4 WARN=0 ERROR=0 SKIP=0 TOTAL=4

fal run

# ali
# gorkem
```

## Run script per model

Alternatively instead of running this script once for the whole project, you may want to run it for a specific model.
In that case you would have to change the script to act on a single model.

```
model_meta = context.current_model.meta
if model_meta:
    print(model_meta["owner"])
```

Next modfiy the trigger to invoke the script to run with the context of the models you choose.

```yaml
models:
  - name: boston
    meta:
      owner: "@ali"
      fal:
        scripts:
          - fal_scripts/list_model_owner.py

  - name: los angeles
    meta:
      owner: "@gorkem"
      fal:
        scripts:
          - fal_scripts/list_model_owner.py
```

Similary invoke fal cli after a succcesful dbt run

```
dbt run

# Found 4 models, 0 tests, 0 snapshots, 0 analyses, 184 macros, 0 operations, 1 seed file, 0 sources, 0 exposures
# Completed successfully
# Done. PASS=4 WARN=0 ERROR=0 SKIP=0 TOTAL=4

fal run

# ali
# gorkem
```
