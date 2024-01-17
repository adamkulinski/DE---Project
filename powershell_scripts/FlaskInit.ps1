$project_location = [Environment]::GetEnvironmentVariable('PROJECT_LOCATION', 'User')

Set-Location $project_location

python "${project_location}\ps1_executor_api\app.py"