"""
DEPENDENCY: ref("model_b")
- ref('model_a')
"""
# weird way to call, but added in the docstring
df = ref(*["model_b"])

df["my_bool"] = True

write_to_model(df)
