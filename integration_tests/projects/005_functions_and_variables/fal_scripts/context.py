from fal.typing import *
from _fal_testing.utils import create_dynamic_artifact

assert context.current_model

extra = ""
extra += f"context: {context}\n"
extra += f"model: {context.current_model}\n"

response = context.current_model.adapter_response
assert response

extra += f"adapter response: {response}\n"
extra += f"adapter response: rows affected {response.rows_affected}\n"

create_dynamic_artifact(context, additional_data=extra)
