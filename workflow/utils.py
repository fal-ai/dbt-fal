from google.cloud import bigquery

def get_table_as_df(table_name):
    bqclient = bigquery.Client()

    # Download query results.
    query_string = f"""
    select * from `learning-project-305919.dbt_burkay.{table_name}`
    """

    dataframe = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    return dataframe
