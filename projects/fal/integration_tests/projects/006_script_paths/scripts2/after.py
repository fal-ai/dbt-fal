import os
from functools import reduce
import pandas as pd
import io


def write_data(data, model_name):
    temp_dir = os.getenv("temp_dir", ".")

    write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".after2.txt"]), "w")
    write_dir.write(data)
    write_dir.close()


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


process_data(context, ref)
