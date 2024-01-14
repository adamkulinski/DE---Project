from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Define the DAG
dag = DAG(
    'first_data_load',
    default_args=default_args,
    description='DAG for making a HTTP POST request to run a PS1 script',
    schedule_interval=None,
    start_date=datetime(2024, 1, 14),
    is_paused_upon_creation=True,
    catchup=False,
)

# Define the HTTP POST tasks:

# Task for clearing stage tables
clear_tables_task = SimpleHttpOperator(
    task_id='post_sql_script',
    http_conn_id='host_flask_api',  # custom connection on docker.host.internal
    endpoint='run-sql-script',  # method name on flask api
    method='POST',
    headers={"Content-Type": "application/json"},
    data='{"sql_script_name": "clear_stage_tables.sql"}',
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

# Task for initial data load
ingest_data_task = SimpleHttpOperator(
    task_id='run_ps1_script',
    http_conn_id='host_flask_api',  # custom connection on docker.host.internal
    endpoint='run-ps1-script',  # method name on flask api
    method='POST',
    headers={"Content-Type": "application/json"},
    data='{"script_name": "InitStageLoad.ps1"}',
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

# Set the task dependency
clear_tables_task >> ingest_data_task
