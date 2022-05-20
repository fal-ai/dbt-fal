import pandas as pd
import io

print("relative import in inner directories")
from ..my_utils import write_data

print("use 'utils' as base even in inner directories")
from utils.my_utils import write_data


def process_data(context, ref):
    model_name = context.current_model.name

    output = f"Model name: {model_name}"
    output = output + f"\nStatus: {context.current_model.status}"

    df: pd.DataFrame = ref(model_name)
    buf = io.StringIO()
    df.info(buf=buf, memory_usage=False)
    info = buf.getvalue()

    output = output + f"\nModel dataframe information:\n{info}"
    write_data(output, model_name)
