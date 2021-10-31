from prophet import Prophet
from utils import get_table_as_df

class ProphetAction:
    def __init__(self):
        self.model = Prophet()
        return

    def run(self, context):
        df = context.get_input('df')
        y = context.get_input('y')
        ds = context.get_input('ds')
        periods = int(context.get_input('periods'))
        
        dataframe = get_table_as_df(df)

        dataframe["y"] = dataframe[y]
        dataframe["ds"] = dataframe[ds]

        self.model.fit(dataframe)
        future = self.model.make_future_dataframe(periods=periods)
        forecast = self.model.predict(future)

        print(forecast)

        context.set_output('status', 'SUCCESS')
        return
