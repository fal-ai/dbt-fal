"""Send an forecasted data plot to Slack.

Dependencies:
 - slack_sdk
 - fbprophet

Follow instructions in slack.py for setting up a minimal Slack bot.

This example is built for a model that has two columns: y and ds, where
y is a metric measure and ds is a timestamp.

The metric that we look at is Agent Wait Time in minutes.
"""

import os
import pandas as pd
import datetime
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from fbprophet import Prophet
from fbprophet.plot import plot_plotly


FORECAST_PREFIX = "fal_forecast_"
CHANNEL_ID = os.getenv("SLACK_BOT_CHANNEL")
SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")


def make_forecast(dataframe: pd.DataFrame, filename: str):
    """Make forecast on metric data."""
    m = Prophet()
    m.fit(dataframe)

    n_future_days = 30
    ds = dataframe["ds"].max()
    future_dates = []
    for _ in range(n_future_days):
        ds = ds + datetime.timedelta(days=1)
        future_dates.append(ds)
    df_future = pd.DataFrame({"ds": future_dates})
    forecast = m.predict(df_future)
    fig = plot_plotly(m, forecast, xlabel="Date", ylabel="Agent Wait Time")
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


def forecast(message):
    """Make forecast on a model and send plot to Slack."""
    df = ref(context['current_model']['name'])
    forecast = make_forecast(
        dataframe=df, filename=f"{FORECAST_PREFIX}{time.time()}.png"
    )
    send_slack_file(
        file_path=forecast,
        message_text=message,
        channel_id=CHANNEL_ID,
        slack_token=SLACK_TOKEN,
    )


forecast('zendesk forecast')
