import pandas as pd
import io
from utils.my_util import handle_dbt_test, model_info_str

model_info = model_info_str(context.current_model)
model_name = context.current_model.name
output = f"Model name: {model_name}"

if context.current_model.status == "tested":
    handle_dbt_test(context.current_model.tests, output, model_name)

else:
    output = f"Model name: {model_name}"
    output = output + f"\nStatus: {context.current_model.status}"

    df: pd.DataFrame = ref(model_name)
    buf = io.StringIO()
    df.info(buf=buf, memory_usage=False)
    info = buf.getvalue()

    output = output + f"\nModel dataframe information:\n{info}"

    with open(f"temp/{model_name}", "w") as f:
        f.write(output)

    # TODO: Pass information to outside of this script
    RESULT = df.size

print(f"Script test.py done for {model_info}")
