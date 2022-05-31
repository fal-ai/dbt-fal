# `fal-dagster` package

Run fal inside a Dagster project.

## Install
You need [poetry](https://python-poetry.org/) in order to build the package wheel.

From this directory, build a wheel:

```
poetry build
```
And install it:
```
pip install dist/fal_dagster-0.1.0-py3-none-any.whl
```

## Try example project

Navigate inside the `example` dir.

Build the user code Docker image:
```bash
docker build -f Dockerfile_fal .
```

Once the build is complete, store the image ID as an environment variable:

```bash
export DAGSTER_CURRENT_IMAGE=my_image_id
```

Also set the dbt and dagster project dir variables:

```bash
export DBT_PROJECT_DIR=path_to_example_dir
export DAGSTER_PROJECT_DIR=path_to_example_dbt_project_dir
```

Inside `dagster.yaml`, change the volumes mapping in run_launcher:

```yaml
    ...
    container_kwargs:
      auto_remove: true
      volumes:
        - /path_to_example_dbt_project_dir:/opt/dbt_project
        - /path_to_example_dir:/opt/dagster/app
```

Start docker-compose:

```bash
docker-compose up
```

Some warnings might appear, we will resolve them later.

Now, in your browser, you can navigate to http://localhost:3000 and launch a run from there.

`repo.py` is an example of how a user might use `fal_dagster`.
