from __future__ import annotations

from contextlib import contextmanager, nullcontext
from functools import partial
from typing import Any, Callable, Iterator

import pandas as pd
from dbt.adapters.base import BaseAdapter
from dbt.adapters.fal.python import PythonAdapter
from dbt.config import RuntimeConfig
from dbt.contracts.connection import AdapterResponse
from fal.packages.environments import BaseEnvironment

from .adapter_support import (
    prepare_for_adapter,
    read_relation_as_df,
    reconstruct_adapter,
    reload_adapter_cache,
    write_df_to_relation,
)
from .connections import FalConnectionManager
from .utils import fetch_environment, retrieve_symbol


def _run_with_adapter(code: str, adapter: BaseAdapter) -> Any:
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
    return _run_with_adapter(code, adapter)


def run_in_environment(
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


class FalAdapter(PythonAdapter):
    ConnectionManager = FalConnectionManager

    @classmethod
    def type(cls):
        return "fal"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    def submit_python_job(
        self, parsed_model: dict, compiled_code: str
    ) -> AdapterResponse:
        """Execute the given `compiled_code` in the target environment."""
        environment_name = parsed_model["config"].get(
            "fal_environment",
            self.credentials.default_environment,
        )

        environment, is_local = fetch_environment(
            self.config.project_root, environment_name
        )

        if is_local:
            return _run_with_adapter(compiled_code, self._db_adapter)

        with self._invalidate_db_cache():
            return run_in_environment(environment, compiled_code, self.config)

    @contextmanager
    def _invalidate_db_cache(self) -> Iterator[None]:
        try:
            yield
        finally:
            # Since executed Python code might alter the database
            # layout, we need to regenerate the relations cache
            # after every time we execute a Python model.
            #
            # TODO: maybe propagate a list of tuples with the changes
            # from the Python runner, so that we can tell the cache
            # manager about what is going on instead of hard-resetting
            # the cache-db.
            reload_adapter_cache(self._db_adapter, self.config)

    @property
    def credentials(self) -> Any:
        python_creds = self.config.python_adapter_credentials
        # dbt-fal is not configured as a Python adapter,
        # maybe we should raise an error?
        assert python_creds is not None
        return python_creds
