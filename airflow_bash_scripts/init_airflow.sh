#!/bin/bash

# Airflow SQL database
airflow db init

# Wait for Airflow webserver to start
sleep 10

# Create a user (Change the details as per your requirement)
airflow users create \
  --username admin \
  --password admin \
  --firstname John \
  --lastname Doe \
  --role Admin \
  --email johndoe@example.com || true

# Start the Airflow web server
exec airflow webserver