from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.fal.connections import TeleportCredentials, TeleportTypeEnum

from dbt.fal.adapters.teleport.impl import TeleportAdapter
from dbt.fal.adapters.teleport.info import TeleportInfo


class DuckDBAdapterTeleport(TeleportAdapter):

    def __init__(self, db_adapter: BaseAdapter, teleport_credentials: TeleportCredentials):
        self._db_adapter = db_adapter
        self.credentials = teleport_credentials
        with self._db_adapter.connection_named('teleport:init'):
            self._db_adapter.execute("INSTALL 'parquet'")
            self._db_adapter.execute("INSTALL httpfs")
            self._db_adapter.execute("LOAD 'parquet'")

    @classmethod
    def storage_formats(cls):
        return ['parquet']

    def teleport_from_external_storage(self, relation: BaseRelation, relation_path: str, teleport_info: TeleportInfo):
        assert teleport_info.format == 'parquet', "duckdb only supports parquet format for Teleport"

        url = teleport_info.build_url(relation_path)

        with self._db_adapter.connection_named('teleport:copy_from'):
            if self.credentials.type == TeleportTypeEnum.REMOTE_S3:
                # Putting this in __init__ didn't work, looks like it has to be done with each new connection
                self._setup_s3()

            rendered_macro = self._db_adapter.execute_macro(
                'duckdb__copy_from_parquet',
                kwargs={'relation': relation, 'url': url})
            self._db_adapter.execute(rendered_macro)

    def teleport_to_external_storage(self, relation: BaseRelation, teleport_info: TeleportInfo):
        assert teleport_info.format == 'parquet', "duckdb only supports parquet format for Teleport"
        rel_path = teleport_info.build_relation_path(relation)
        url = teleport_info.build_url(rel_path)
        with self._db_adapter.connection_named('teleport:copy_to'):
            if self.credentials.type == TeleportTypeEnum.REMOTE_S3:
                self._setup_s3()
            rendered_macro = self._db_adapter.execute_macro('duckdb__copy_to', kwargs={'relation': relation, 'url': url})
            self._db_adapter.execute(rendered_macro)

        return rel_path

    def _setup_s3(self):
        self._db_adapter.execute("LOAD httpfs")
        self._db_adapter.execute(f"SET s3_region='{self.credentials.s3_region}'")
        self._db_adapter.execute(f"SET s3_access_key_id='{self.credentials.s3_access_key_id}'")
        self._db_adapter.execute(f"SET s3_secret_access_key='{self.credentials.s3_access_key}'")
