from _fal_testing import create_model_artifact

# weird way to call, but added in the docstring
df = ref("model_a1")
df["b1_data"] = 1
write_to_model(df)

create_model_artifact(context)
