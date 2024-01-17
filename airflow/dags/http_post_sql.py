from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',  # who is the owner of the DAG
    'depends_on_past': False,  # whether to run the DAG if the previous task failed
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # how many times to retry the task
}

# Define the DAG
dag = DAG(
    'template_dag',
    default_args=default_args,
    description="""
    This is a template DAG.
    """,
    schedule_interval='@daily',  # how often to run the DAG
    start_date=datetime(2024, 1, 15),  # when to start running the DAG
    is_paused_upon_creation=True,  # whether to start the DAG as paused
    catchup=False,  # whether to run the DAG for the past dates
)

# A dummy task to start the DAG
start = BashOperator(task_id="start", bash_command="echo start", dag=dag)

# Task for clearing stage tables
clear_tables_task = SimpleHttpOperator(  # SimpleHttpOperator is used for calling Flask API endpoints
    task_id='post_sql_script',  # task name
    http_conn_id='host_flask_api',  # custom connection on docker.host.internal
    endpoint='run-sql-script',  # method name on flask api
    method='POST',
    headers={"Content-Type": "application/json"},
    data='{"sql_script_name": "test_sql_select.sql"}',  # which sql script to run
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

# A dummy task to start the DAG
end = BashOperator(task_id="end", bash_command="echo end", dag=dag)

(start
 >> clear_tables_task
 >> end
 )
