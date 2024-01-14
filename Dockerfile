FROM apache/airflow:latest

# Copy the initialization script
COPY airflow_bash_scripts/init_airflow.sh /init_airflow.sh
