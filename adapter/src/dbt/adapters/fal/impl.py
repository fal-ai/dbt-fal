from __future__ import annotations

import dbt.exceptions
from contextlib import contextmanager, nullcontext
from functools import partial
from typing import Any, Callable, Iterator, Dict, Any

import pandas as pd
from dbt.parser.manifest import Manifest, ManifestLoader, MacroManifest
from dbt.adapters.base import BaseAdapter
from dbt.adapters.fal.python import PythonAdapter
from dbt.config import RuntimeConfig
from dbt.contracts.connection import AdapterResponse

from .adapter_support import (
    prepare_for_adapter,
    read_relation_as_df,
    reconstruct_adapter,
    reload_adapter_cache,
    write_df_to_relation,
)
from .connections import FalConnectionManager, FalCredentials
from .utils import fetch_environment, retrieve_symbol
from .environments import create_environment, run_function, iter_logs

# Delay between polls.
POLL_DELAY = 0.1


def _isolated_runner(
    code: str,
    config: RuntimeConfig,
    manifest: Manifest,
    macro_manifest: MacroManifest,
) -> Any:
    # This function is going to run in an entirely different machine.
    adapter = reconstruct_adapter(config, manifest, macro_manifest)
    main = retrieve_symbol(code, "main")
    return main(
        read_df=prepare_for_adapter(adapter, read_relation_as_df),
        write_df=prepare_for_adapter(adapter, write_df_to_relation),
    )


def run_in_environment(
    credentials: FalCredentials,
    manifest: Manifest,
    macro_manifest: MacroManifest,
    kind: str,
    configuration: Dict[str, Any],
    code: str,
    config: RuntimeConfig,
) -> AdapterResponse:
    """Run the 'main' function inside the given code on the
    specified environment."""

    if not credentials.fal_api_base:
        raise dbt.exceptions.RuntimeException(
            "The 'fal_api_base' field is required for running fal environments."
            "\nYou can start your own fal server by running 'docker run -p 5000:5000"
            " fal-ai/isolate-server' and\nsetting 'fal_api_base' to 'http://localhost:5000'."
        )

    # User's environment
    environment_token = create_environment(
        credentials.fal_api_base, kind, configuration
    )

    # Start running the entrypoint function in the remote environment.
    status_token = run_function(
        credentials.fal_api_base,
        environment_token,
        partial(_isolated_runner, code, config, manifest, macro_manifest),
    )

    for log in iter_logs(credentials.fal_api_base, status_token):
        if log["source"] == "user":
            print(f"[{log['level']}]", log["message"])
        elif log["source"] == "builder":
            print(f"[environment builder] [{log['level']}]", log["message"])
        elif log["source"] == "bridge":
            print(f"[environment bridge] [{log['level']}]", log["message"])

    # TODO: we should somehow tell whether the run was successful or not.
    return AdapterResponse("OK")


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

        # local => exec() or subprocess?
        # local isolate => venv creation
        # remote isolate => venv creation docker

        assert environment_name != "local", "local running is disabled."

        environment_definition = fetch_environment(
            self.config.project_root, environment_name
        )

        kind = environment_definition.pop("kind")
        if kind == "venv":
            # Alias venv to virtualenv, as isolate calls it.
            kind = "virtualenv"

        with self._invalidate_db_cache():
            return run_in_environment(
                self.credentials,
                self.manifest,
                self.macro_manifest,
                kind,
                environment_definition,
                compiled_code,
                self.config,
            )

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
            reload_adapter_cache(self._db_adapter, self.manifest)

    @property
    def credentials(self) -> Any:
        python_creds = self.config.python_adapter_credentials
        # dbt-fal is not configured as a Python adapter,
        # maybe we should raise an error?
        assert python_creds is not None
        return python_creds

    @property
    def manifest(self) -> Manifest:
        return ManifestLoader.get_full_manifest(self.config)

    @property
    def macro_manifest(self) -> MacroManifest:
        return self._db_adapter.load_macro_manifest()
