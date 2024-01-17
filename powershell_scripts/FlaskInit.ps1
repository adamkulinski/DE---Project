$project_location = [Environment]::GetEnvironmentVariable('PROJECT_LOCATION', 'User')

Set-Location $project_location

python "${project_location}\job_executor_api\main.py"