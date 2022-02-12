# Example 5: Incorporate fal into a CI/CD pipeline
You can use fal as part of your CI/CD pipeline. In this example we use [Github Actions](https://github.com/features/actions).

## Environment variables
Environment variables need to be provided as [repository secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets) These will be different depending on which sources and outputs your project uses. In this example we use [BigQuery](https://cloud.google.com/bigquery/) both as a source and as an output, and so we need to provide BigQuery-specific environment variables:

- `SERVICE_ACCOUNT_KEY`: contents of a `keyfile.json`. For more information see [here](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
- `GCLOUD_PROJECT`: your Google Cloud project ID, necessary for seeding
- `BQ_DATASET`: name of the BigQuery dataset, necessary for seeding

If your fal scripts require environment variables, these should also be provided as repository secrets.

## Setup dbt project

### `profiles.yml`
Since every dbt setup has it's own `profiles.yml`, usually in `~/.dbt` directory, there can be discrepancies between different setups. We therefore need a standard `profiles.yml` that will be used in the CI/CD workflow. Here's a profiles.yml that is specific to this example project:

```yaml
fal_dbt_examples:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: "{{ env_var('KEYFILE_DIR') }}/keyfile.json"
      project: "{{ env_var('GCLOUD_PROJECT') }}"
      dataset: "{{ env_var('BQ_DATASET') }}"
      threads: 1
      timeout_seconds: 300
      location: US
      priority: interactive
```

As you can see, it uses [environment variables](#environment-variables) for some properties, as it's best to keep these secret.

### `requirements.txt`
All the packages that are necessary to run dbt and fal should be put in `requirements.txt`. This includes any packages that are used by user-defined python scripts. Here's an example of how `requirements.txt` can look like:

```
# Core dependencies
dbt-core
dbt-bigquery
fal

# Script dependencies
slack_sdk
datadog_api_client
```

## Action Workflow

### Install dependencies
The first step in our workflow is to setup python and install the dependencies from `requirements.txt`:

```yaml
- uses: actions/setup-python@v2
  with:
    python-version: '3.8.x'

- name: Install dependencies
  run: |
    pip install --upgrade --upgrade-strategy eager -r requirements.txt
```

### Make secret key available
`keyfile.json` data needs to be stored in a file and provided as a variable:

```yaml
- name: Setup secret key
  env:
    SERVICE_ACCOUNT_KEY: ${{ secrets.SERVICE_ACCOUNT_KEY }}
  run: |
    echo "$SERVICE_ACCOUNT_KEY" > $HOME/keyfile.json
    ls -la $HOME/keyfile.json
    echo 'keyfile is ready'
```
Note the use of secrets.

### Setup variables and run scripts
Finally, we setup the necessary environment variables and trigger dbt and fal runs:

```yaml
- name: Run dbt and fal
  env:
    GCLOUD_PROJECT: ${{ secrets.GCLOUD_PROJECT }}
    BQ_DATASET: ${{ secrets.BQ_DATASET }}
    SLACK_BOT_TOKEN: ${{ secrets.SLACK_BOT_TOKEN }}
    SLACK_BOT_CHANNEL: ${{ secrets.SLACK_BOT_CHANNEL }}
    DD_API_KEY: ${{ secrets.DD_API_KEY }}
    DD_APP_KEY: ${{ secrets.DD_APP_KEY }}
  run: |
    export KEYFILE_DIR=$HOME
    dbt seed
    dbt run --profiles-dir .
    fal run --profiles-dir .
```

## Full example
The full example of incorporating dbt and fal in a CI/CD pipeline can be found in [our example repository](https://github.com/fal-ai/fal_dbt_examples).
