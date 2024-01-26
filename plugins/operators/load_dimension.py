from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class LoadDimensionOperator(BaseOperator):
    """
    This operator is expected to be able to insert new data to dimension tables with a truncate-insert functionality.
    """

    ui_color = "#80BD9E"
    truncate_sql = "TRUNCATE TABLE {};"
    insert_sql = "INSERT INTO {} ({});"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        target_table="",
        truncate=True,
        *args,
        **kwargs,
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(
                f"Truncating existing data from {self.target_table} dimension table"
            )
            formated_truncate_sql = LoadDimensionOperator.truncate_sql.format(
                self.target_table
            )
            redshift_hook.run(formated_truncate_sql)

        sql_queries = SqlQueries()
        insert_query = sql_queries.get_insert_query(self.target_table)

        self.log.info(f"Inserting new data to {self.target_table} dimension table")
        formatted_insert_sql = LoadDimensionOperator.insert_sql.format(
            self.target_table,
            insert_query,
        )
        redshift_hook.run(formatted_insert_sql)
