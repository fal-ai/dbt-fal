from dbt.adapters.base import AdapterPlugin

from dbt.adapters.fal_enc.connections import FalEncCredentials
from dbt.adapters.fal_enc.impl import FalEncAdapter
from dbt.include import fal_enc

# NOTE: include_path has links to dbt-fal adapter's for Teleport
Plugin = AdapterPlugin(adapter=FalEncAdapter, credentials=FalEncCredentials, include_path=fal_enc.PACKAGE_PATH)
