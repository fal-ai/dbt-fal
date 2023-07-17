from fal.dbt.typing import *
from _fal_testing import create_dynamic_artifact, get_environment_type

df = ref('model_c')
print(f"Model c has {len(df)} rows")

create_dynamic_artifact(context, additional_data=f"Environment: {get_environment_type()}")
