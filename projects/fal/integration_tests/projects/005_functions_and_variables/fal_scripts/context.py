from __future__ import annotations

from _fal_testing.utils import create_dynamic_artifact

from fal.typing import *

assert context.current_model

extra = ""
extra += f"context: {context}\n"
extra += f"model: {context.current_model}\n"
extra += f"target: {context.target}\n"
extra += f"target name: {context.target.name}\n"
extra += f"target profile: {context.target.profile_name}\n"
extra += f"target database: {context.target.database}\n"

response = context.current_model.adapter_response
assert response

extra += f"adapter response: {response}\n"
extra += f"adapter response: rows affected {response.rows_affected}\n"

create_dynamic_artifact(context, additional_data=extra)
