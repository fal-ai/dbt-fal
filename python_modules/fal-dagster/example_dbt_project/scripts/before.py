import pandas as pd
import io

model_name = context.current_model.name

output = f"Model name: {model_name}"
output = output + f"\nStatus: {context.current_model.status}"

df: pd.DataFrame = ref(model_name)
buf = io.StringIO()
df.info(buf=buf, memory_usage=False)
info = buf.getvalue()

output = output + f"\nModel dataframe information:\n{info}"
print(output)
