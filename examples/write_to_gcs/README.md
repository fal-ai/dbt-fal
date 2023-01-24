# Example 9: Write dbt artifacts to Google Cloud Storage

Dbt artifacts are files created by the dbt compiler after a run is completed. They contain information about the project, help with documentation, calculate test coverage and much more. In this example we are going to focus on two of these artifacts `manifest.json` and `run_results.json`.

[`manifest.json`](https://docs.getdbt.com/reference/artifacts/manifest-json) Is a full point-in-time represantation of your dbt project. This file can be used to pass [state](https://docs.getdbt.com/docs/guides/understanding-state) to a dbt run using the `--state` flag.

[`run_results.json`](https://docs.getdbt.com/reference/artifacts/run-results-json) file contains timing and status information about a completed dbt run.

There might be several reasons why might want to store `dbt` artifacts. The most obvious reason would be to use the `--state` functionality to pass the previous state back to dbt. Besides that these artifacts can be stored in a database to later be analyzed..

This example assumes you have GCS (Google Cloud Storage) enabled in your project.

## Create a GCS Bucket

Navigate the [GCS console](https://console.cloud.google.com/storage/browser) and create a bucket. In the screenshot below I named the my bucket `fal_example_dbt_artifacts_bucket`, pick a unique name for yourself and complete the next steps as you see fit.

![GCS bucket creation](gcs_bucket.png)

## Fal Script

Now navigate to your dbt to project, create a directory for your fal scripts and create a python file inside that directory. [For example](https://github.com/fal-ai/fal_dbt_examples/tree/main/fal_scripts/upload_to_gcs.py) a directory named `fal_scripts` and a python file named `upload_to_gcs.py`.

In your python file you'll write the script that would upload `dbt` artifacts to GSC.

```python
import os
from google.cloud import storage

bucket_name = "fal_example_dbt_artifacts_bucket"
manifest_destination_blob_name = "manifest.json"
run_results_destination_blob_name = "run_results.json"

manifest_source_file_name = os.path.join(context.config.target_path, "manifest.json")
run_results_source_file_name = os.path.join(context.config.target_path, "run_results.json")

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
manifest_blob = bucket.blob(manifest_destination_blob_name)
run_results_blob = bucket.blob(run_results_destination_blob_name)

manifest_blob.upload_from_filename(manifest_source_file_name)
run_results_blob.upload_from_filename(run_results_source_file_name)
```

This script will use [default credentials](https://cloud.google.com/docs/authentication/production) set in your environment for GCP.

## Meta tag

Next, we need to configure when this script will run. This part will be different for every usecase. Currently there are two types of triggers, a script can be configured per model or globally for the whole project. In this example we want this script to run once, not for any specific model.

To configure this navigate to your `schema.yml` file or create one if you dont have one and add the following yaml entry.

```yaml
fal:
  scripts:
    - fal_scripts/upload_to_gcs.py
```

## Full example

You can find the full code example [here](https://github.com/fal-ai/fal_dbt_examples/blob/main/fal_scripts).
