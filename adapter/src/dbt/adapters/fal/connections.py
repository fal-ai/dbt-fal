from typing import Tuple

from dbt.adapters.base import Credentials
from dbt.adapters.python.connections import PythonConnectionManager


class FalConnectionManager(PythonConnectionManager):
    TYPE = "fal"

    @classmethod
    def open(cls, connection):
        raise NotImplementedError

    def execute(self, compiled_code: str):
        raise NotImplementedError

    def cancel(self, connection):
        raise NotImplementedError


class FalCredentials(Credentials):
    default_environment: str = "local"

    @property
    def type(self):
        return "fal"

    def _connection_keys(self):
        return ()
