"""
DAG for executing the data validation and ingestion pipeline.
Performs CDC operations on the data.
Executes the following tasks:
    - clean_init_ingest_task_group:
        - clean_ingest_client_data_task: Task for validating and ingesting client data
        - clean_ingest_household_data_task: Task for validating and ingesting household data
        - clean_ingest_income_data_task: Task for validating and ingesting income data
        - clean_ingest_loan_data_task: Task for validating and ingesting loan data
    - customer_car_cdc: Task for CDC on client data
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
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
    'data_validation_and_ingestion_pipeline',
    default_args=default_args,
    description="""
    DAG for executing the data validation and ingestion pipeline.
    Performs CDC operations on the data.
    """,
    schedule_interval='@daily',  # Run the DAG daily at midnight
    start_date=datetime(2024, 1, 15),
    is_paused_upon_creation=True,
    catchup=False,
)

# A dummy task to start the DAG
start = BashOperator(task_id="start", bash_command="echo start", dag=dag)

with TaskGroup(group_id='clear_and_load_to_stage', dag=dag) as clear_and_load_to_stage:

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

    load_updated_data = SimpleHttpOperator(
        task_id='load_updated_data_stage',
        http_conn_id='host_flask_api',
        endpoint='run-ps1-script',
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"script_name": "NewDataLoad.ps1"}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

# Tasks groups for the DAG:

# Task group for cleaning and ingesting data
with TaskGroup(group_id='clean_init_ingest', dag=dag) as clean_init_ingest:
    # Task for validating and ingesting client data
    clean_ingest_client_data_task = SimpleHttpOperator(
        task_id='clean_ingest_client_data',
        http_conn_id='host_flask_api',
        endpoint='run-sql-script',
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"sql_script_name": "client_data_cleaning_new_rows.sql"}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    # Task for validating and ingesting household data
    clean_ingest_household_data_task = SimpleHttpOperator(
        task_id='clean_ingest_household_data',
        http_conn_id='host_flask_api',
        endpoint='run-sql-script',
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"sql_script_name": "household_data_cleaning_new_rows.sql"}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    # Task for validating and ingesting income data
    clean_ingest_income_data_task = SimpleHttpOperator(
        task_id='clean_ingest_income_data',
        http_conn_id='host_flask_api',
        endpoint='run-sql-script',
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"sql_script_name": "income_data_cleaning_new_rows.sql"}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    # Task for validating and ingesting loan data
    clean_ingest_loan_data_task = SimpleHttpOperator(
        task_id='clean_ingest_loan_data',
        http_conn_id='host_flask_api',
        endpoint='run-sql-script',
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"sql_script_name": "loan_data_cleaning_new_rows.sql"}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

# Task group for cleaning and ingesting data
with TaskGroup(group_id='car_customer_table', dag=dag) as car_customer_update:
    # Task for CDC on client data
    customer_car_data_update = SimpleHttpOperator(
        task_id='customer_car_data_update',
        http_conn_id='host_flask_api',
        endpoint='run-sql-script',
        method='POST',
        headers={"Content-Type": "application/json"},
        data='{"sql_script_name": "customer_car_update.sql"}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

# A dummy task to start the DAG
end = BashOperator(task_id="end", bash_command="echo end", dag=dag)


# Tasks dependencies
(start
 >> clear_tables_task
    >> load_updated_data
    >> clean_ingest_client_data_task
        >> clean_ingest_household_data_task
        >> clean_ingest_income_data_task
        >> clean_ingest_loan_data_task
        >> car_customer_update
 >> end
 )
