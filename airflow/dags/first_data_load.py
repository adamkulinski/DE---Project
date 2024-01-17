"""
This DAG is used to perform the first data load into the data warehouse.

Uses these SQL scripts:
    - clear_stage_tables.sql
    - client_data_cleaning_init.sql
    - household_data_cleaning_init.sql
    - income_data_cleaning_init.sql
    - loan_data_cleaning_init.sql
    - customer_car_update.sql
And these PowerShell scripts:
    - InitStageLoad.ps1
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Define the default arguments for the DAG
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
    description='Pipeline for the first data load',
    schedule_interval=None,
    start_date=datetime(2024, 1, 14),
    is_paused_upon_creation=True,
    catchup=False,
)

start = BashOperator(
    task_id='start',
    bash_command='echo "Data validation and ingestion started"',
    dag=dag,
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

with TaskGroup(group_id='data_validation', dag=dag) as data_validation:

    # Task for clearing stage tables
    validate_client_data = SimpleHttpOperator(
        task_id='validate_client_data',
        http_conn_id='host_flask_api',  # custom connection on docker.host.internal
        endpoint='run-sql-script',  # method name on flask api
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"sql_script_name": "client_data_cleaning_init.sql"}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    # Task for clearing stage tables
    validate_household_data = SimpleHttpOperator(
        task_id='validate_household_data',
        http_conn_id='host_flask_api',  # custom connection on docker.host.internal
        endpoint='run-sql-script',  # method name on flask api
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"sql_script_name": "household_data_cleaning_init.sql"}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    # Task for clearing stage tables
    validate_income_data = SimpleHttpOperator(
        task_id='validate_income_data',
        http_conn_id='host_flask_api',  # custom connection on docker.host.internal
        endpoint='run-sql-script',  # method name on flask api
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"sql_script_name": "income_data_cleaning_init.sql"}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    # Task for clearing stage tables
    validate_loan_data = SimpleHttpOperator(
        task_id='validate_loan_data',
        http_conn_id='host_flask_api',  # custom connection on docker.host.internal
        endpoint='run-sql-script',  # method name on flask api
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"sql_script_name": "loan_data_cleaning_init.sql"}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

# Task for clearing stage tables
customer_car_first_load = SimpleHttpOperator(
    task_id='customer_car_first_load',
    http_conn_id='host_flask_api',  # custom connection on docker.host.internal
    endpoint='run-sql-script',  # method name on flask api
    method='POST',
    headers={"Content-Type": "application/json"},
    data='{"sql_script_name": "customer_car_ingest_init.sql"}',
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

end = BashOperator(
    task_id='end',
    bash_command='echo "Data validation complete"',
    dag=dag,
)

# Set the task dependency
(start
 >> clear_tables_task
 >> ingest_data_task
    >> validate_client_data
    >> validate_household_data
    >> validate_income_data
    >> validate_loan_data
 >> customer_car_first_load
 >> end
 )
