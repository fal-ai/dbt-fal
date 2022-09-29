from dataclasses import dataclass
from typing import Tuple

from dbt.adapters.base import Credentials
from dbt.adapters.fal.python import PythonConnectionManager


class FalConnectionManager(PythonConnectionManager):
    TYPE = "fal"

    @classmethod
    def open(cls, connection):
        raise NotImplementedError

    def execute(self, compiled_code: str):
        raise NotImplementedError

    def cancel(self, connection):
        raise NotImplementedError


@dataclass
class FalCredentials(Credentials):
    default_environment: str = "local"

    # NOTE: So we can not set them in profiles.yml
    # they are ignored for now
    database: str = ''
    schema: str = ''

    @property
    def type(self):
        return "fal"

    def _connection_keys(self):
        return ()
