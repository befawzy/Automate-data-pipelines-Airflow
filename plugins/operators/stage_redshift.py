from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    This operator is expected to be able to load any JSON-formatted files from S3 to Amazon Redshift.
    """

    ui_color = "#358140"
    create_staging_sql = """
    CREATE {} IF NOT EXIST
    {}"""

    copy_staging_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
    """

    @apply_defaults
    def __init__(
        self,
        table="",
        redshift_conn_id="redshift",
        aws_credentials_id="",
        access_key="",
        secret_access="",
        s3_bucket="",
        s3_key="",
        json_log_file="",
        *args,
        **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.access_key = access_key
        self.secret_access = secret_access
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_log_file = json_log_file
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info("StageToRedshiftOperator not implemented yet")

        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        json_path = "s3://{}/{}".format(self.s3_bucket, self.json_log_file)
        formatted_sql = StageToRedshiftOperator.copy_staging_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            json_path,
        )
        redshift.run(formatted_sql)
