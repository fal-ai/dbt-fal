"""Forecast and upload order data
Packages:
 - prophet
"""

import pandas as pd
from prophet import Prophet
import sqlalchemy.types as types


def make_forecast(dataframe: pd.DataFrame, periods: int = 30):
    """Make forecast on metric data."""
    model = Prophet(daily_seasonality=False, yearly_seasonality=False)
    model.fit(dataframe)

    future = model.make_future_dataframe(periods=periods)
    prediction = model.predict(future)

    return model, prediction


def plot_forecast(model: Prophet, forecast: pd.DataFrame, filename: str):
    from prophet.plot import plot_plotly

    fig = plot_plotly(model, forecast)
    fig.write_image(f"{context.current_model.name}_{filename}.jpg")


df: pd.DataFrame = ref("orders_daily")
print(df)

df_count = df[["order_date", "order_count"]]
df_count = df_count.rename(columns={"order_date": "ds", "order_count": "y"})
model_count, forecast_count = make_forecast(df_count, 50)
# plot_forecast(model_count, forecast_count, "count")

df_amount = df[["order_date", "order_amount"]]
df_amount = df_amount.rename(columns={"order_date": "ds", "order_amount": "y"})
model_amount, forecast_amount = make_forecast(df_amount, 50)
# plot_forecast(model_amount, forecast_amount, "amount")

joined_forecast = forecast_count.join(
    forecast_amount.set_index("ds"),
    on="ds",
    lsuffix="_count",
    rsuffix="_amount",
)
print(joined_forecast.dtypes)

# HACK: have to figure out how to write dates (or datetimes) to the database
# TODO: The types.DATE did not work when testing for `dtype={"ds": types.DATE}`
joined_forecast["ds"] = joined_forecast["ds"].map(lambda x: x.strftime("%Y-%m-%d"))

# Generates a table with a BUNCH of columns
# It will use the current model as target, no need to pass it
write_to_model(joined_forecast, mode="overwrite")
