from fal.typing import *
from _fal_testing.utils import create_dynamic_artifact

assert context.current_model

lines = []

for model in list_models():
    lines.append(
        f"model: {model.name} property: {model.meta['property']['other'] if model.meta else None}"
    )

for source in list_sources():
    lines.append(
        f"source: {source.name} {source.table_name} property: {source.meta['property']['other'] if source.meta else None}"
    )

create_dynamic_artifact(context, additional_data="\n".join(lines))
