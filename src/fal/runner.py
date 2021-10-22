"""Run forecasts with FAL."""
import click
import os
import yaml
import json
import pandas as pd
import datetime
import time
import pathlib
from glob import glob
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from fbprophet import Prophet
from fbprophet.plot import plot_plotly
from google.cloud import bigquery, bigquery_storage
from typing import List


DBT_PROJECT_YML = "dbt_project.yml"
DBT_MANIFEST_JSON = "manifest.json"
FORECAST_PREFIX = "fal_forecast_"
CHANNEL_ID = "C01GYF1KWV7"
SLACK_TOKEN = os.environ["SLACK_BOT_TOKEN"]


@click.command()
def run():
    """Run forecast."""
    try:
        with open(DBT_PROJECT_YML) as raw_project:
            project = yaml.load(raw_project, Loader=yaml.FullLoader)
            project_dir = str(pathlib.Path().resolve())
            # Find all YMLs
            paths = [filename for item in os.walk(str(project_dir)) for filename in glob(os.path.join(item[0], '*.yml'))]

            # Find model names
            model_names = [_find_model_names(path) for path in paths]
            model_names = [model for sublist in model_names for model in sublist]

            # Run forecast
            for model in model_names:
                _forecast(model=model, message=f"Forecast for {model}", project=project)

    except (OSError, yaml.YAMLError):
        raise OSError(
            f"File {DBT_PROJECT_YML} not found")


def _find_model_names(filepath) -> List[str]:
    with open(filepath) as rawyml:
        tree = yaml.load(rawyml, Loader=yaml.FullLoader)
        output = []
        for key in tree:
            if 'models' in tree:
                for model in tree['models']:
                    if 'meta' in model and 'fal_model' in model['meta']:
                        output.append(model['name'])
        return list(dict.fromkeys(output))


def _forecast(model, message, project: dict):
    target_path = project['target-path']
    model_table = find_model_table(
        model_name=model, target_path=target_path)
    df = get_bq_df(table_id=model_table)
    forecast = make_forecast(
        dataframe=df,
        filename=f"{FORECAST_PREFIX}{time.time()}.png")
    send_slack_file(
        file_path=forecast,
        message_text=message,
        channel_id=CHANNEL_ID,
        slack_token=SLACK_TOKEN
    )


def find_model_table(model_name: str, target_path: str):
    """Find BigQuery table for a given model."""
    with open(f"{target_path}/{DBT_MANIFEST_JSON}") as raw_manifest:
        manifest_data = json.load(raw_manifest)
        model_key_list = list(filter(
            lambda key: model_name in key,
            list(manifest_data["nodes"].keys())))
        assert len(model_key_list) == 1, "Couldn't find a unique model"
        relation = manifest_data["nodes"][model_key_list[0]]["relation_name"]
        return ''.join(c for c in relation if c != '`')


def get_bq_df(table_id: str):
    """Get BigQuery table as a dataframe."""
    bq_client = bigquery.Client()
    table = bigquery.TableReference.from_string(table_id)
    rows = bq_client.list_rows(table)
    bq_storage_client = bigquery_storage.BigQueryReadClient()
    return rows.to_dataframe(bqstorage_client=bq_storage_client)


def make_forecast(dataframe: pd.DataFrame, filename: str):
    """Make forecast on monthly data."""
    m = Prophet(daily_seasonality=False, weekly_seasonality=False)
    m.fit(dataframe)

    n_future_months = 20
    begin_ds = dataframe['ds'].max()
    month = begin_ds.month
    year = begin_ds.year
    future_dates = []
    for _ in range(n_future_months):
        month += 1
        if month > 12:
            month = 1
            year += 1
        future_dates.append(datetime.datetime(year=year, month=month, day=1))
    df_future = pd.DataFrame({'ds': future_dates})
    forecast = m.predict(df_future)
    fig = plot_plotly(m, forecast, xlabel='Month', ylabel='Ozone (ppm)')
    fig.write_image(filename)
    return filename


def send_slack_file(
        file_path: str,
        message_text: str,
        channel_id: str,
        slack_token: str):
    """Send file to slack."""
    client = WebClient(token=slack_token)

    try:
        client.files_upload(
            channels=channel_id,
            file=file_path,
            title="FAL forecast",
            initial_comment=message_text
        )
    except SlackApiError as e:
        assert e.response["error"]
