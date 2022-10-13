from __future__ import annotations

from functools import partial
from typing import Any

from dbt.adapters.base.impl import BaseAdapter
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.connection import AdapterResponse

from fal.packages.environments import BaseEnvironment

from .adapter_support import (
    prepare_for_adapter,
    read_relation_as_df,
    reconstruct_adapter,
    write_df_to_relation,
)

from .utils import retrieve_symbol

def run_with_adapter(code: str, adapter: BaseAdapter) -> Any:
    # main symbol is defined during dbt-fal's compilation
    # and acts as an entrypoint for us to run the model.
    main = retrieve_symbol(code, "main")
    return main(
        read_df=prepare_for_adapter(adapter, read_relation_as_df),
        write_df=prepare_for_adapter(adapter, write_df_to_relation),
    )

def _isolated_runner(code: str, config: RuntimeConfig) -> Any:
    # This function can be run in an entirely separate
    # process or an environment, so we need to reconstruct
    # the DB adapter solely from the config.
    adapter = reconstruct_adapter(config)
    return run_with_adapter(code, adapter)

def run_in_environment_with_adapter(
    environment: BaseEnvironment,
    code: str,
    config: RuntimeConfig,
) -> AdapterResponse:
    """Run the 'main' function inside the given code on the
    specified environment.

    The environment_name must be defined inside fal_project.yml file
    in your project's root directory."""

    with environment.connect() as connection:
        execute_model = partial(_isolated_runner, code, config)
        result = connection.run(execute_model)
        return result
