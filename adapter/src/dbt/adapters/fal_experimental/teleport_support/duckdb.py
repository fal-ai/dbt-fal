from contextlib import contextmanager
from typing import Optional
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.fal_experimental.connections import TeleportCredentials, TeleportTypeEnum

from dbt.fal.adapters.teleport.impl import TeleportAdapter
from dbt.fal.adapters.teleport.info import TeleportInfo
from dbt.exceptions import RuntimeException


class DuckDBAdapterTeleport(TeleportAdapter):
    def __init__(
        self, db_adapter: BaseAdapter, teleport_credentials: TeleportCredentials
    ):
        self._db_adapter = db_adapter
        self.credentials = teleport_credentials
        with self._db_adapter.connection_named("teleport:init"):
            self._db_adapter.execute("INSTALL parquet")
            self._db_adapter.execute("INSTALL httpfs")

    @classmethod
    def storage_formats(cls):
        return ["parquet"]

    def teleport_from_external_storage(
        self, relation: BaseRelation, relation_path: str, teleport_info: TeleportInfo
    ):
        assert (
            teleport_info.format == "parquet"
        ), "duckdb only supports parquet format for Teleport"

        url = teleport_info.build_url(relation_path)

        with self._db_adapter.connection_named("teleport:copy_from"):
            if self.credentials.type == TeleportTypeEnum.LOCAL:
                rendered_macro = self._db_adapter.execute_macro(
                    "duckdb__copy_from_parquet",
                    kwargs={"relation": relation, "url": url},
                )
                self._db_adapter.execute(rendered_macro)

            elif self.credentials.type == TeleportTypeEnum.REMOTE_S3:
                with self._s3_setup():
                    rendered_macro = self._db_adapter.execute_macro(
                        "duckdb__copy_from_parquet",
                        kwargs={"relation": relation, "url": url},
                    )
                    self._db_adapter.execute(rendered_macro)
            else:
                raise RuntimeError(
                    f"Teleport type {self.credentials.type} not supported"
                )

    def teleport_to_external_storage(
        self, relation: BaseRelation, teleport_info: TeleportInfo
    ):
        assert (
            teleport_info.format == "parquet"
        ), "duckdb only supports parquet format for Teleport"

        rel_path = teleport_info.build_relation_path(relation)
        url = teleport_info.build_url(rel_path)

        with self._db_adapter.connection_named("teleport:copy_to"):
            if self.credentials.type == TeleportTypeEnum.LOCAL:
                rendered_macro = self._db_adapter.execute_macro(
                    "duckdb__copy_to", kwargs={"relation": relation, "url": url}
                )
                self._db_adapter.execute(rendered_macro)
            elif self.credentials.type == TeleportTypeEnum.REMOTE_S3:
                with self._s3_setup():
                    rendered_macro = self._db_adapter.execute_macro(
                        "duckdb__copy_to", kwargs={"relation": relation, "url": url}
                    )
                    self._db_adapter.execute(rendered_macro)
            else:
                raise RuntimeError(
                    f"Teleport type {self.credentials.type} not supported"
                )

        return rel_path

    def _get_setting(self, name: str):
        try:
            _, table = self._db_adapter.execute(
                f"SELECT current_setting('{name}')", fetch=True
            )
            return table.rows[0][0]
        except RuntimeException:
            return None

    def _set_setting(self, name: str, value: Optional[str]):
        if value:
            self._db_adapter.execute(f"SET {name} = '{value}'")
        else:
            # HACK while we get a response https://github.com/duckdb/duckdb/issues/4998
            self._db_adapter.execute(f"SET {name} = ''")

    @contextmanager
    def _s3_setup(self):
        self._db_adapter.execute("LOAD parquet")
        self._db_adapter.execute("LOAD httpfs")

        old_region = self._get_setting("s3_region")
        old_access_key_id = self._get_setting("s3_access_key_id")
        old_secret_access_key = self._get_setting("s3_secret_access_key")

        self._set_setting("s3_region", self.credentials.s3_region)
        self._set_setting("s3_access_key_id", self.credentials.s3_access_key_id)
        self._set_setting("s3_secret_access_key", self.credentials.s3_access_key)

        yield

        self._set_setting("s3_region", old_region)
        self._set_setting("s3_access_key_id", old_access_key_id)
        self._set_setting("s3_secret_access_key", old_secret_access_key)
