from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'http_post_request_dag',
    default_args=default_args,
    description='DAG for making a HTTP POST request',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

post_task = SimpleHttpOperator(
    task_id='post_sql_script',
    http_conn_id='http_post_example',
    endpoint='runscript',
    method='POST',
    headers={"Content-Type": "application/json"},
    data='{"sql_script_name": "test_sql_select.sql"}',
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

post_task
