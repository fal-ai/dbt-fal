from dbt.adapters.base import AdapterPlugin

from dbt.adapters.fal_experimental.connections import FalCredentials
from dbt.adapters.fal_experimental.impl import FalAdapter
from dbt.include import fal_experimental

Plugin = AdapterPlugin(
    adapter=FalAdapter,
    credentials=FalCredentials,
    include_path=fal_experimental.PACKAGE_PATH,
)
