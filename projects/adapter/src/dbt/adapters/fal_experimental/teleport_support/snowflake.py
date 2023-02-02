from typing import List
from dbt.adapters.fal_experimental.connections import TeleportCredentials
from dbt.fal.adapters.teleport.impl import TeleportAdapter
from dbt.fal.adapters.teleport.info import TeleportInfo
from dbt.adapters.fal_experimental.adapter_support import new_connection
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.impl import BaseAdapter


class SnowflakeAdapterTeleport(TeleportAdapter):

    def __init__(self, db_adapter: BaseAdapter, teleport_credentials: TeleportCredentials):
        from dbt.adapters.fal_experimental.adapter_support import new_connection
        self._db_adapter = db_adapter
        self._credentials = teleport_credentials

        # TODO: put this in teleport_info
        url = f's3://{teleport_credentials.s3_bucket}/teleport'

        with new_connection(self._db_adapter, "fal-snowflake:setup-teleport") as conn:
            cur = conn.handle.cursor()

            create_stage_query = f"""CREATE OR REPLACE STAGE falstage
            URL = '{url}' CREDENTIALS = (
            aws_key_id='{self._credentials.s3_access_key_id}',
            aws_secret_key='{self._credentials.s3_access_key}');"""

            create_format_query = """CREATE OR REPLACE FILE FORMAT falparquet type = 'PARQUET';"""

            cur.execute(create_stage_query)
            cur.execute(create_format_query)


    @classmethod
    def storage_formats(cls):
        return ['parquet']

    def teleport_from_external_storage(self, relation: BaseRelation, relation_path: str, teleport_info: TeleportInfo) -> None:
        assert teleport_info.format == 'parquet', "snowflake only supports parquet format for Teleport"
        location = f"@falstage/{relation_path}"

        with self._db_adapter.connection_named('teleport:copy_from'):
            create_macro = self._db_adapter.execute_macro(
                'snowflake__create_table_from_parquet',
                kwargs={
                    'relation': relation,
                    'location': location,
                })

            self._db_adapter.execute(create_macro)

            copy_macro = self._db_adapter.execute_macro(
                'snowflake__copy_from_parquet',
                kwargs={
                    'relation': relation,
                    'location': location,
                }
            )
            self._db_adapter.execute(copy_macro)

    def teleport_to_external_storage(self, relation: BaseRelation, teleport_info: TeleportInfo) -> str:
        assert teleport_info.format == 'parquet', "snowflake only supports parquet format for Teleport"
        relation_path = teleport_info.build_relation_path(relation)
        location = f"@falstage/{relation_path}"
        rendered_macro = self._db_adapter.execute_macro(
            'snowflake__copy_to_parquet',
            kwargs={
                'relation': relation,
                'location': location
            })
        with self._db_adapter.connection_named('teleport:copy_to'):
            self._db_adapter.execute(rendered_macro)
        return relation_path

