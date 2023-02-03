from __future__ import annotations

from _fal_testing.utils import create_dynamic_artifact

from fal.typing import *

assert context.current_model

lines = []

for model in list_models():
    lines.append(
        f"model: {model.name} property: {model.meta['property']['other'] if model.meta else None}"
    )

for source in list_sources():
    lines.append(
        # NOTE: removing the namespace prefix
        f"source: {source.name} {source.table_name.split('__ns__')[1]} property: {source.meta['property']['other'] if source.meta else None}"
    )

create_dynamic_artifact(context, additional_data="\n".join(lines))
