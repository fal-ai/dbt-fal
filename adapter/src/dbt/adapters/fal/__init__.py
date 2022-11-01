from dbt.adapters.base import AdapterPlugin

from dbt.adapters.fal.connections import FalEncCredentials
from dbt.adapters.fal.impl import FalEncAdapter
from dbt.include import fal

Plugin = AdapterPlugin(
    adapter=FalEncAdapter,
    credentials=FalEncCredentials,
    include_path=fal.PACKAGE_PATH
)
