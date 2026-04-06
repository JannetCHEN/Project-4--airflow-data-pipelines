from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'jannet',
    'start_date': pendulum.now(), 
    'depends_on_past': False, 
    'retries': 3, 
    'retry_delay': timedelta(minutes=5), 
    'catchup': False, 
    'email_on_retry': False, 
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    stop_operator = DummyOperator(task_id='Stop_execution')

    # LOAD STAGING TABLES
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table="staging_events",
        s3_bucket="jannet-data-pipelines",
        s3_key="log-data",
        json_path="log_json_path.json", 
        iam_role="arn:aws:iam::605170323675:role/my-redshift-service-role",
        region="us-east-1"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table="staging_songs",
        s3_bucket="jannet-data-pipelines",
        s3_key="song-data/",  
        json_path="auto",  
        iam_role="arn:aws:iam::605170323675:role/my-redshift-service-role",
        region="us-east-1"
    )


    # LOAD FACT TABLES
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        target_table="songplay",
        append_only=False,
        sql_query=final_project_sql_statements.SqlQueries.songplay_table_insert
    )

    # LOAD DIMENSION TABLES
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.user_table_insert,
        redshift_conn_id='redshift',
        target_table="user_info",
        truncate=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.song_table_insert,
        redshift_conn_id='redshift',
        target_table="song"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.artist_table_insert,
        redshift_conn_id='redshift',
        target_table="artist"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.time_table_insert,
        redshift_conn_id='redshift',
        target_table="time"
    )

    # DATA QUALITY CHECKS
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        sql_queries = [
            "SELECT COUNT(*) FROM songplay WHERE songplay_id IS NULL",
            "SELECT COUNT(*) FROM user_info WHERE userid IS NULL",
            "SELECT COUNT(*) FROM song WHERE song_id IS NULL",  
            "SELECT COUNT(*) FROM artist WHERE artist_id IS NULL",  
            "SELECT COUNT(*) FROM time WHERE start_time IS NULL"
        ],
        expected_results = [
            0, 
            0, 
            0,
            0,
            0
        ]
    )

    # TASK DEPENDANCIES
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> stop_operator

final_project_dag = final_project()
