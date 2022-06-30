import pandas as pd
import os

df = pd.DataFrame(
    {
        "my_array": [["some", "other"], []],
        "other_array": [[1, 2, 3], []],
    }
)

df.info()

write_to_model(df)

model_name = context.current_model.name
df = ref(model_name)
output = f"my_array: {df['my_array'][0]}"
output += f"\nother_array: {df['other_array'][0]}"
temp_dir = os.environ["temp_dir"]
with open(os.path.join(temp_dir, model_name + ".txt"), "w") as file:
    file.write(output)
