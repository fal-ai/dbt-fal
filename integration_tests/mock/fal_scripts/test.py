import pandas as pd
import io
import my_util

model_name = context.current_model.name

output = f"Model name: {model_name}"

if context.current_model.status == "tested":
    my_util.handle_dbt_test(context.current_model.tests, output, model_name)

else:
    output = f"Model name: {model_name}"
    output = output + f"\nStatus: {context.current_model.status}"

    df: pd.DataFrame = ref(model_name)
    buf = io.StringIO()
    df.info(buf=buf, memory_usage=False)
    info = buf.getvalue()

    output = output + f"\nModel dataframe information:\n{info}"

    f = open(f"temp/{model_name}", "w")
    f.write(output)
    f.close()
