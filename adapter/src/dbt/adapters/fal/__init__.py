from dbt.adapters.base import AdapterPlugin

from dbt.adapters.fal.connections import FalCredentials
from dbt.adapters.fal.impl import FalAdapter
from dbt.include import fal

Plugin = AdapterPlugin(
    adapter=FalAdapter, credentials=FalCredentials, include_path=fal.PACKAGE_PATH
)
