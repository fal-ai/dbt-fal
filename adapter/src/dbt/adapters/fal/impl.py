from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from dbt.contracts.connection import AdapterResponse
from dbt.parser.manifest import MacroManifest, Manifest, ManifestLoader

from dbt.adapters.fal.python import PythonAdapter

from .adapter_support import reload_adapter_cache
from .connections import FalConnectionManager
from .environments import read_env_definition, run_on_host_machine, run_on_local_machine


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
        env_name = parsed_model["config"].get(
            "fal_environment",
            self.credentials.default_environment,
        )
        env_definition = read_env_definition(self.config.project_root, env_name)

        kind = env_definition.pop("kind")
        if self.credentials.host and kind != "local":
            runner = run_on_host_machine
        else:
            runner = run_on_local_machine

        with self._invalidate_db_cache():
            return runner(
                self.credentials,
                kind,
                env_definition,
                compiled_code,
                model_state={
                    "config": self.config,
                    "manifest": self.manifest,
                    "macro_manifest": self.macro_manifest,
                },
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
