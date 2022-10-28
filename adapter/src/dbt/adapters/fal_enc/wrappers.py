from typing import Any, Optional, Type
from dbt.adapters.fal.impl import FalAdapterMixin
from dbt.adapters.factory import get_adapter_by_type
from dbt.adapters.base.impl import BaseAdapter
from dbt.contracts.connection import Credentials
from fal.telemetry import telemetry


class FalCredentialsWrapper:
    _db_creds: Optional[Credentials] = None

    def __init__(self, db_creds: Credentials):
        self._db_creds = db_creds

    @property
    def type(self):
        return "fal_enc"

    def __getattr__(self, name: str) -> Any:
        """
        Directly proxy to the DB adapter, just shadowing the type
        """
        return getattr(self._db_creds, name)


class FalEncAdapterWrapper(FalAdapterMixin):
    @telemetry.log_call("encapsulate_init")
    def __init__(self, db_adapter_type: Type[BaseAdapter], config):
        # Use the db_adapter_type connection manager
        self.ConnectionManager = db_adapter_type.ConnectionManager

        db_adapter = get_adapter_by_type(db_adapter_type.type())
        super().__init__(config, db_adapter)

        # HACK: A Python adapter does not have _available_ all the attributes a DB adapter does.
        # Since we use the DB adapter as the storage for the Python adapter, we must proxy to it
        # all the unhandled calls.
        self._available_ = self._db_adapter._available_.union(self._available_)

    @telemetry.log_call("encapsulate_submit_python_job")
    def submit_python_job(self, *args, **kwargs):
        return super().submit_python_job(*args, **kwargs)

    @classmethod
    def type(cls):
        return "fal_enc"

    def __getattr__(self, name):
        """
        Directly proxy to the DB adapter, Python adapter in this case does what we explicitly define in this class.
        """
        if hasattr(self._db_adapter, name):
            return getattr(self._db_adapter, name)
        else:
            getattr(super(), name)
