import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'a. avramov',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkify_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
)

start_operator = DummyOperator(
    task_id='Begin',
    dag=dag
)

drop_artists = PostgresOperator(
    task_id='drop_artists',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/drop_artists.sql',
)

drop_songplays = PostgresOperator(
    task_id='drop_songplays',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/drop_songplays.sql',
)

drop_songs = PostgresOperator(
    task_id='drop_songs',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/drop_songs.sql',
)

drop_staging_events = PostgresOperator(
    task_id='drop_staging_events',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/drop_staging_events.sql',
)

drop_staging_songs = PostgresOperator(
    task_id='drop_staging_songs',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/drop_staging_songs.sql',
)

drop_time = PostgresOperator(
    task_id='drop_time',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/drop_time.sql',
)

drop_users = PostgresOperator(
    task_id='drop_users',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/drop_users.sql',
)

deletion_creation_pause = DummyOperator(
    task_id='dummy_pause',
    dag=dag
)

create_artists = PostgresOperator(
    task_id='create_artists',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/create_artists.sql',
)

create_songplays = PostgresOperator(
    task_id='create_songplays',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/create_songplays.sql',
)

create_songs = PostgresOperator(
    task_id='create_songs',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/create_songs.sql',
)

create_staging_events = PostgresOperator(
    task_id='create_staging_events',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/create_staging_events.sql',
)

create_staging_songs = PostgresOperator(
    task_id='create_staging_songs',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/create_staging_songs.sql',
)

create_time = PostgresOperator(
    task_id='create_time',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/create_time.sql',
)

create_users = PostgresOperator(
    task_id='create_users',
    dag=dag,
    postgres_conn_id='redshift',
    sql='/table_creation/create_users.sql',
)

creation_staging_pause = DummyOperator(
    task_id='creation_staging_pause',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_path='s3://airflow-course-data-aaa/2018/11/',
    json="s3://airflow-course-data-aaa/log_json_path.json", # https://knowledge.udacity.com/questions/838478
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_path='s3://airflow-course-data-aaa/A/A/', # tried with copying the data to my s3 bucket, but ran out of space...
    json="auto", # https://knowledge.udacity.com/questions/838478
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_table',
    dag=dag,
    redshift_conn_id='redshift',
    query=SqlQueries.songplay_table_insert,
    table='songplays',
    load_method = 'append'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_dim_user_table',
    dag=dag,
    redshift_conn_id='redshift',
    query=SqlQueries.user_table_insert,
    table='users',
    load_method = 'overwrite'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_dim_song_table',
    dag=dag,
    redshift_conn_id='redshift',
    query=SqlQueries.song_table_insert,
    table='songs',
    load_method = 'overwrite'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_dim_artist_table',
    dag=dag,
    redshift_conn_id='redshift',
    query=SqlQueries.artist_table_insert,
    table='artists',
    load_method = 'overwrite'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_dim_time_table',
    dag=dag,
    redshift_conn_id='redshift',
    query=SqlQueries.time_table_insert,
    table='time',
    load_method = 'overwrite'
)

run_quality_checks = DataQualityOperator(
    task_id='run_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(
    task_id='finish',
    dag=dag
)

start_operator >> [
    drop_artists,
    drop_songplays,
    drop_songs,
    drop_staging_events,
    drop_staging_songs,
    drop_time,
    drop_users
] >> deletion_creation_pause >> [
    create_artists,
    create_songplays,
    create_songs,
    create_staging_events,
    create_staging_songs,
    create_time,
    create_users
] >> creation_staging_pause >> [
    stage_events_to_redshift,
    stage_songs_to_redshift
] >> load_songplays_table >> [
    load_user_dimension_table, 
    load_song_dimension_table, 
    load_artist_dimension_table, 
    load_time_dimension_table
] >> run_quality_checks >> end_operator
