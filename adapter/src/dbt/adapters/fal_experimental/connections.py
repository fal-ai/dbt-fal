from dataclasses import dataclass
from typing import Optional
import os

from dbt.adapters.base import Credentials
from dbt.dataclass_schema import StrEnum, ExtensibleDbtClassMixin

from dbt.fal.adapters.python import PythonConnectionManager


class TeleportTypeEnum(StrEnum):
    LOCAL = "local"
    REMOTE_S3 = "s3"

@dataclass
class TeleportCredentials(ExtensibleDbtClassMixin):
    type: TeleportTypeEnum

    # local
    local_path: Optional[str] = os.getcwd()

    # s3
    s3_bucket: Optional[str] = None
    s3_region: Optional[str] = None
    s3_access_key_id: Optional[str] = None
    s3_access_key: Optional[str] = None


class FalConnectionManager(PythonConnectionManager):
    TYPE = "fal_experimental"

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
    teleport: Optional[TeleportCredentials] = None
    host: str = ''
    key_secret: str = ''
    key_id: str = ''


    # NOTE: So we are allowed to not set them in profiles.yml
    # they are ignored for now
    database: str = ''
    schema: str = ''

    @property
    def type(self):
        return "fal_experimental"

    def _connection_keys(self):
        return ()
