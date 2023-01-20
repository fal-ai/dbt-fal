from typing import Any, Optional, Type, Set

from dbt.adapters.factory import get_adapter_by_type
from dbt.adapters.base.meta import available
from dbt.adapters.base.impl import BaseAdapter
from dbt.contracts.connection import Credentials
from dbt.parser.manifest import ManifestLoader
from dbt.clients.jinja import MacroGenerator

from ..fal_experimental.utils import cache_static
from ..fal_experimental.impl import FalAdapterMixin
from ..fal_experimental import telemetry


class FalCredentialsWrapper:
    _db_creds: Optional[Credentials] = None

    def __init__(self, db_creds: Credentials):
        self._db_creds = db_creds

    @property
    def type(self):
        if find_funcs_in_stack({"to_target_dict", "db_materialization"}):
            # This makes sense for both SQL and Python because the target is always the db
            return self._db_creds.type

        return "fal"

    def __getattr__(self, name: str) -> Any:
        """
        Directly proxy to the DB adapter, just shadowing the type
        """
        return getattr(self._db_creds, name)


class FalEncAdapterWrapper(FalAdapterMixin):
    def __init__(self, db_adapter_type: Type[BaseAdapter], config):
        # Use the db_adapter_type connection manager
        self.ConnectionManager = db_adapter_type.ConnectionManager

        db_adapter = get_adapter_by_type(db_adapter_type.type())
        super().__init__(config, db_adapter)

        # HACK: A Python adapter does not have self._available_ all the attributes a DB adapter does.
        # Since we use the DB adapter as the storage for the Python adapter, we must proxy to it
        # all the unhandled calls.

        # self._available_ is set by metaclass=AdapterMeta
        self._available_ = self._db_adapter._available_.union(self._available_)
        # self._parse_replacements_ is set by metaclass=AdapterMeta
        self._parse_replacements_.update(self._db_adapter._parse_replacements_)

        telemetry.log_api(
            "encapsulate_init",
            config=config,
            additional_props={"is_teleport": self.is_teleport()},
        )

    def submit_python_job(self, *args, **kwargs):
        return super().submit_python_job(*args, **kwargs)

    @available
    @telemetry.log_call(
        "encapsulate_db_materialization", log_args=["materialization"], config=True
    )
    def db_materialization(self, context: dict, materialization: str):
        # NOTE: inspired by https://github.com/dbt-labs/dbt-core/blob/be4a91a0fe35a619587b7a0145e190690e3771c6/core/dbt/task/run.py#L254-L290
        materialization_macro = self.manifest.find_materialization_macro_by_name(
            self.config.project_name, materialization, self._db_adapter.type()
        )

        # HACK: run the entire SQL materialization and return the resulting dict with relations created
        return MacroGenerator(
            materialization_macro, context, stack=context["context_macro_stack"]
        )()

    @property
    @cache_static
    def manifest(self):
        return ManifestLoader.get_full_manifest(self.config)

    def type(self):
        if find_funcs_in_stack({"render", "db_materialization"}):
            return self._db_adapter.type()

        return "fal"

    def __getattr__(self, name):
        """
        Directly proxy to the DB adapter, Python adapter in this case does what we explicitly define in this class.
        """
        if hasattr(self._db_adapter, name):
            return getattr(self._db_adapter, name)
        else:
            getattr(super(), name)


def find_funcs_in_stack(funcs: Set[str]) -> bool:
    import inspect

    # NOTE: from https://stackoverflow.com/a/42636264/1276441
    frame = inspect.currentframe()
    while frame:
        if frame.f_code.co_name in funcs:
            return True
        frame = frame.f_back

    return False
