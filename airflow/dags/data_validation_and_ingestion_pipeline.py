from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

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
    'data_validation_and_ingestion_pipeline',
    default_args=default_args,
    description='DAG for executing the data validation and ingestion pipeline',
    schedule_interval=None,
    start_date=datetime(2024, 1, 14),
    is_paused_upon_creation=True,
    catchup=False,
)

# Define the HTTP POST tasks:

# Task for validating and ingesting client data
clean_ingest_client_data_task = SimpleHttpOperator(
    task_id='clean_ingest_client_data',
    http_conn_id='host_flask_api',  # custom connection on docker.host.internal
    endpoint='run-sql-script',  # method name on flask api
    method='POST',
    headers={"Content-Type": "application/json"},
    data='{"sql_script_name": "client_data_cleaning.sql"}',
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

# Task for validating and ingesting household data
clean_ingest_household_data_task = SimpleHttpOperator(
    task_id='clean_ingest_household_data',
    http_conn_id='host_flask_api',  # custom connection on docker.host.internal
    endpoint='run-sql-script',  # method name on flask api
    method='POST',
    headers={"Content-Type": "application/json"},
    data='{"sql_script_name": "household_data_cleaning.sql"}',
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

# Task for validating and ingesting income data
clean_ingest_income_data_task = SimpleHttpOperator(
    task_id='clean_ingest_income_data',
    http_conn_id='host_flask_api',  # custom connection on docker.host.internal
    endpoint='run-sql-script',  # method name on flask api
    method='POST',
    headers={"Content-Type": "application/json"},
    data='{"sql_script_name": "income_data_cleaning.sql"}',
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

# Task for validating and ingesting loan data
clean_ingest_loan_data_task = SimpleHttpOperator(
    task_id='clean_ingest_loan_data',
    http_conn_id='host_flask_api',  # custom connection on docker.host.internal
    endpoint='run-sql-script',  # method name on flask api
    method='POST',
    headers={"Content-Type": "application/json"},
    data='{"sql_script_name": "loan_data_cleaning.sql"}',
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)

# Set the task dependency
(clean_ingest_client_data_task
 >> clean_ingest_household_data_task
 >> clean_ingest_income_data_task
 >> clean_ingest_loan_data_task
 )
