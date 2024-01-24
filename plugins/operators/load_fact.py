from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadFactOperator(BaseOperator):
    """
    This operator is expected to be able to append new data to fact tables.
    """

    ui_color = "#F98866"
    insert_sql = "INSERT INTO {} ({});"

    @apply_defaults
    def __init__(self, redshift_conn_id="redshift", target_table="", *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        sql_queries = SqlQueries()
        insert_query = sql_queries.get_insert_query(self.target_table)

        self.log.info(f"Appending new data to {self.target_table} fact table")
        formatted_insert_sql = LoadFactOperator.insert_sql.format(
            self.target_table,
            insert_query,
        )
        redshift_hook.run(formatted_insert_sql)
