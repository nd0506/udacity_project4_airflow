from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'ndd',
    'depends_on_past'=True,
    'email_on_retry': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 1, 12, 0, 0)
}

@dag = DAG('sparkify_pipline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='public.staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='ndd-bucket',
    s3_key='log_data',
    copy_json_option='s3://udacity-dend/log_json_path.json',
    region='us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='public.staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='ndd-bucket',
    s3_key='song_data',
    copy_json_option='auto',
    region='us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.songplays',
    truncate_table=True,
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.users',
    truncate_table=True,
    query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.songs',
    truncate_table=True,
    query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.artists',
    truncate_table=True,
    query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift",
    table='public.time',
    truncate_table=True,
    query=SqlQueries.time_table_insert
)

data_quality_check = [
    {
        "data_quality_check": "SELECT count(songid) FROM songs WHERE songid IS NULL",
        "expectation": 0
    },
    {
        "data_quality_check": "SELECT count(userid) FROM users WHERE userid IS NULL",
        "expectation": 0
    },
   {
        "data_quality_check": "SELECT count(artistid) FROM artists WHERE artistid IS NULL",
        "expectation": 0
    },
   {
        "data_quality_check": "SELECT count(playid) FROM songplays WHERE playid IS NULL",
        "expectation": 0
    },
   {
        "data_quality_check": "SELECT count(start_time) FROM time WHERE start_time IS NULL",
        "expectation": 0
    }
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    data_quality_check=data_quality_check
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table >> [load_user_dimension_table,
                                load_song_dimension_table,
                                load_artist_dimension_table,
                                load_time_dimension_table] \
    >> run_quality_checks >> end_operator
