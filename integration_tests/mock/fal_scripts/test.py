import pandas as pd
import io

model_name = context.current_model.name

output = f"Model name: {model_name}"

if context.current_model.status == "tested":
    for test in context.current_model.tests:
        output += f"\nRan {test.name} for {test.column}, result: {test.status}"
    f = open(f"fal_output/{model_name}_test", "w")
    f.write(output)
    f.close()

else:
    output = f"Model name: {model_name}"
    output = output + f"\nStatus: {context.current_model.status}"

    df: pd.DataFrame = ref(model_name)
    buf = io.StringIO()
    df.info(buf=buf, memory_usage=False)
    info = buf.getvalue()

    output = output + f"\nModel dataframe information:\n{info}"

    f = open(f"fal_output/{model_name}", "w")
    f.write(output)
    f.close()
