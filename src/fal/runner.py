"""Run forecasts with FAL."""
import click
import yaml
import json
import pandas as pd
import datetime
from fbprophet import Prophet
from google.cloud import bigquery, bigquery_storage


DBT_PROJECT_YML = "dbt_project.yml"
DBT_MANIFEST_JSON = "manifest.json"


@click.command()
@click.argument("model")
def run(model: str):
    """Run forecast."""
    try:
        with open(DBT_PROJECT_YML) as raw_project:
            project = yaml.load(raw_project, Loader=yaml.FullLoader)
            target_path = project['target-path']
            model_table = find_model_table(
                model_name=model, target_path=target_path)
            df = get_bq_df(table_id=model_table)
            forecast = make_forecast(dataframe=df)
            print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())
    except (OSError, yaml.YAMLError):
        raise OSError(
            f"File {DBT_PROJECT_YML} not found")


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


def make_forecast(dataframe: pd.DataFrame):
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
    return m.predict(df_future)
