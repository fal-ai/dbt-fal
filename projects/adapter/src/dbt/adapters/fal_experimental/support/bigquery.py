from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.adapters.base.connections import AdapterResponse
from dbt.adapters.fal_experimental.adapter_support import new_connection
import pandas as pd
import connectorx as cx

# [bigquery] extras dependencies
import google.cloud.bigquery as bigquery
from google.cloud.bigquery.job import WriteDisposition

from dbt.adapters.bigquery import BigQueryAdapter, BigQueryConnectionManager

def read_relation_as_df(adapter: BaseAdapter, relation: BaseRelation) -> pd.DataFrame:
    sql = f"SELECT * FROM {relation}"

    assert adapter.type() == "bigquery"

    db_creds = adapter.config.credentials._db_creds
    method = getattr(db_creds, 'method', None)

    # Connectorx only supports service-account json authentication
    if method and method.value == "service-account":
        connection_str = f"bigquery://{db_creds.keyfile}"
        df = cx.read_sql(connection_str, sql)
        return df

    with new_connection(adapter, "fal-bigquery:read_relation_as_df") as conn:

        connection_manager: BaseConnectionManager = adapter.connections  # type: ignore
        client: bigquery.Client = connection_manager.get_thread_connection().handle  # type: ignore

        job = client.query(sql)
        df = job.to_dataframe()

    return df


def write_df_to_relation(
    adapter: BigQueryAdapter,
    data: pd.DataFrame,
    relation: BaseRelation,
) -> AdapterResponse:

    assert adapter.type() == "bigquery"

    project: str = relation.database  # type: ignore
    dataset: str = relation.schema  # type: ignore
    table: str = relation.identifier  # type: ignore

    with new_connection(adapter, "fal-bigquery:write_df_to_relation") as conn:
        connection_manager: BigQueryConnectionManager = adapter.connections
        client: bigquery.Client = conn.handle

        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(project, dataset), table
        )

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=[
                # TODO: offer as a dbt.config parameter?
                # bigquery.SchemaField.from_api_repr(field)
                # for field in (fields_schema or [])
            ],
            source_format="PARQUET",
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
        )

        with connection_manager.exception_handler("START JOB"):
            job = client.load_table_from_dataframe(
                data, table_ref, job_config=job_config
            )

        timeout = connection_manager.get_job_execution_timeout_seconds(conn) or 300

        with connection_manager.exception_handler("LOAD TABLE"):
            adapter.poll_until_job_completes(job, timeout)

        query_table = client.get_table(job.destination)
        num_rows = query_table.num_rows

    # TODO: better AdapterResponse
    return AdapterResponse("OK", rows_affected=num_rows)
