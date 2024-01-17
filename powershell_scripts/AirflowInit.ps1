$project_location = [Environment]::GetEnvironmentVariable('PROJECT_LOCATION', 'User')

Set-Location $project_location

# Run the docker-compose file
docker-compose up --build