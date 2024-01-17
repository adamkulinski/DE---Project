# Piggybank migration project

## 1. Introduction

This project represents a migration of Piggybank's current insfrastucture to a new one,due to the fact that the
current infrastructure is not scalable and is not able to handle the amount of data that
is being processed, a new infrastructure is needed.

## 2. Architecture

### Components of the current architecture:

![Application architecture.png](images%2FApplication%20architecture.png)

### ERD of the current database:

![ERD - Piggybank.png](images%2FERD%20-%20Piggybank.png)

### Execution of data pipeline:

![Execution of data pipeline.png](images%2FExecution%20of%20data%20pipeline.png)


## How to set up the project

### Requirements
- Windows operational system on host machine
- Docker
- Python <= 3.12
- SQL Database Browser (e.g. DB Browser for SQLite)

For getting access to SQLite DB in Excel you need to install SQLite ODBC Driver. You can download it from [here](https://www.ch-werner.de/sqliteodbc/) or [here](https://www.devart.com/odbc/sqlite/excel-sqlite-odbc-connection.html).
Then you need to create a new ODBC connection in Windows. You need to find ODBC Data Sources (64-bit) in Windows search and click on it, click on System DSN tab, click on Add button, select SQLite3 ODBC Driver, click on Finish button, enter Data Source Name (e.g. Piggybank), click on Select button, select Piggybank.db file.


### 1. Clone the repository

```powershell 
git clone https://github.com/adamkulinski/DE---Project.git
```
### 2. Install requirements.txt

```powershell
pip install -r requirements.txt
```

### 3. Change directory to the project folder in InitConfigScript.ps1

```powershell
[Environment]::SetEnvironmentVariable('PROJECT_LOCATION', '[YOUR_PROJECT_LOCATION]', 'User')
```

### 4. Run InitConfigScript.ps1

```powershell
.\[YOUR_PROJECT_LOCATION]\InitConfigScript.ps1
```
This script will also detect if SQLite tools are downloaded and if not, it will download them.
Also it will map their location to the PATH variable, so **_RunSqlScript.ps1_** can run your SQL scripts.

### 5. Create a new SQLite database called _Piggybank.db_

Open command prompt and run this command:

```powershell
sqlite3 [YOUR_PROJECT_LOCATION]\db\Piggybank.db
```

## Usage

### Creating DAGs

If you want to add a new DAG, create a new **_.py_** file in **_'airflow/dags'_**.
It's possible that Airflow web server will need a refresh on the main paige to see the new DAG.

Example of a DAG is in folder **_'airflow/dags/http_post_sql.py'_**.


Use '>>' operators for setting order of task execution for DAGs.

### Creating SQL jobs

If you want to create a new SQL job for SQLite to execute, put it in **_'/sqlite_jobs'_** folder.

### Creating Powershell scripts

If you want to create a new Powershell script, put it in **_'/powershell_scripts'_** folder.

### Running DAGs

After running the **_InitConfigScript.ps1_** the Airflow container with Flask API will be started.
You can access the Airflow web server on **_localhost:8080_** and use these default credentials to log in:
- username: **_airflow_**
- password: **_airflow_**

On **_localhost:8080_** you should see the newly added DAGs. You can run them by clicking on the **_Trigger DAG_** button or disable it by clicking on the **_Pause/Unpause_** button.

## Shutting down the project

### 1. Run StopServices.ps1

This will stop all the containers and remove them.

```powershell
.\[YOUR_PROJECT_LOCATION]\StopServices.ps1
```

### 2. Stop Flask API

When running **_InitConfigScript.ps1_** the Flask API will start on a new terminal window. You need to close or press Ctrl+C on that window to stop the Flask API.
