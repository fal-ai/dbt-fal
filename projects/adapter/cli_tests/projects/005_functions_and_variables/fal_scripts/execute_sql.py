from fal.dbt.typing import *
from _fal_testing.utils import create_dynamic_artifact

df = execute_sql('SELECT 1 as a, 2 as b, 3 as c')
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns

assert 2 == df['b'][0]

df.info()

create_dynamic_artifact(context)
