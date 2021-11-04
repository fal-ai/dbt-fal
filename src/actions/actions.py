import os
import pandas as pd
import datetime
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from fbprophet import Prophet
from fbprophet.plot import plot_plotly
from typing import List
import click

from faldbt.project import DbtProject


DBT_PROJECT_YML = "dbt_project.yml"
DBT_MANIFEST_JSON = "manifest.json"
FORECAST_PREFIX = "fal_forecast_"
CHANNEL_ID = "C01GYF1KWV7"
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")


def forecast(message, project: DbtProject):
    location = project.find_model_location(project.models[0])
    click.echo(location)
    df = project.get_data_frame(project.find_model_location(project.models[0]))
    forecast = make_forecast(
        dataframe=df, filename=f"{FORECAST_PREFIX}{time.time()}.png"
    )
    send_slack_file(
        file_path=forecast,
        message_text=message,
        channel_id=CHANNEL_ID,
        slack_token=SLACK_TOKEN,
    )


def make_forecast(dataframe: pd.DataFrame, filename: str):
    """Make forecast on monthly data."""
    m = Prophet(daily_seasonality=False, weekly_seasonality=False)
    m.fit(dataframe)

    n_future_months = 20
    begin_ds = dataframe["ds"].max()
    month = begin_ds.month
    year = begin_ds.year
    future_dates = []
    for _ in range(n_future_months):
        month += 1
        if month > 12:
            month = 1
            year += 1
        future_dates.append(datetime.datetime(year=year, month=month, day=1))
    df_future = pd.DataFrame({"ds": future_dates})
    forecast = m.predict(df_future)
    fig = plot_plotly(m, forecast, xlabel="Month", ylabel="Ozone (ppm)")
    fig.write_image(filename)
    return filename


def send_slack_file(
    file_path: str, message_text: str, channel_id: str, slack_token: str
):
    """Send file to slack."""
    client = WebClient(token=slack_token)

    try:
        client.files_upload(
            channels=channel_id,
            file=file_path,
            title="FAL forecast",
            initial_comment=message_text,
        )
    except SlackApiError as e:
        assert e.response["error"]
