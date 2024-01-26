from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)

default_args = {
    "owner": "Beshoy Fawzy",
    "start_date": pendulum.now(),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": True,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)
def sparkify_main_dag():
    with open("/opt/airflow/plugins/helpers/create_tables.sql", "r") as file:
        sql_query = file.read()

    create_tables_task = PostgresOperator(
        task_id="create_table_task",
        postgres_conn_id="your_postgres_conn_id",  # Replace with your Postgres connection ID
        sql=sql_query,
    )

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_key="events",
        json_path="'auto'",
    )
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_key="songs",
        json_path="s3://udacity-dend/log_json_path.json",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        target_table="songplays",
        redshift_conn_id="redshift",
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        target_table="user",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        target_table="song",
        redshift_conn_id="redshift",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        target_table="artist",
        redshift_conn_id="redshift",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        target_table="time",
        redshift_conn_id="redshift",
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        tables=["songplays", "songs", "artists", "time", "users"],
    )
    end_operator = DummyOperator(task_id="Stop_execution")

    start_operator >> create_tables_task
    create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
        load_user_dimension_table,
    ]
    [
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
        load_user_dimension_table,
    ] >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = sparkify_main_dag()
