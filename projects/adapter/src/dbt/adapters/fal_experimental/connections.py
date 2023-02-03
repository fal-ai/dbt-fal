from __future__ import annotations

import os
from dataclasses import dataclass

from dbt.adapters.base import Credentials
from dbt.dataclass_schema import ExtensibleDbtClassMixin, StrEnum
from dbt.fal.adapters.python import PythonConnectionManager


class TeleportTypeEnum(StrEnum):
    LOCAL = "local"
    REMOTE_S3 = "s3"


@dataclass
class TeleportCredentials(ExtensibleDbtClassMixin):
    type: TeleportTypeEnum

    # local
    local_path: str | None = os.getcwd()

    # s3
    s3_bucket: str | None = None
    s3_region: str | None = None
    s3_access_key_id: str | None = None
    s3_access_key: str | None = None


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
    teleport: TeleportCredentials | None = None
    host: str = ""
    key_secret: str = ""
    key_id: str = ""

    # NOTE: So we are allowed to not set them in profiles.yml
    # they are ignored for now
    database: str = ""
    schema: str = ""

    @property
    def type(self):
        return "fal_experimental"

    def _connection_keys(self):
        return ()
