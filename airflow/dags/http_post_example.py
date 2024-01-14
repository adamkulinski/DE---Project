from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

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
    schedule_interval=timedelta(seconds=5),
    start_date=datetime(2024, 1, 14),
    is_paused_upon_creation=True,  # Disable the DAG upon creation
    catchup=False,
)

post_task = SimpleHttpOperator(
    task_id='post_sql_script',
    http_conn_id='host_flask_api',  # custom connection on docker.host.internal
    endpoint='run-sql-script',  # method name on flask api
    method='POST',
    headers={"Content-Type": "application/json"},
    data='{"sql_script_name": "test_sql_select.sql"}',
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

post_task
